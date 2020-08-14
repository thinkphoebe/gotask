package gotask

import "C"
import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"bytes"

	"github.com/coreos/etcd/clientv3"
	"github.com/thinkphoebe/goetcd"
	log "github.com/thinkphoebe/golog"
)

type _TaskInfo struct {
	param            TaskParam
	status           TaskStatus
	foundTime        int64
	rentTime         int64
	lastOORTime      int64 // last out of resource time, used for log print
	leaseId          clientv3.LeaseID
	cancelOwnerWatch context.CancelFunc
	cancelParamWatch context.CancelFunc
	errorCleaned     bool
}

type TaskWorker struct {
	config TaskWorkerConfig

	etcd     goetcd.Etcd
	chTask   chan *_TaskInfo
	chRemove chan *_TaskInfo

	// watch到的任务队列，每taskType一个队列
	taskQueue map[string]*list.List

	// 已获取Owner正在处理的任务的信息
	taskProcessing map[string]*_TaskInfo
	mutex          sync.Mutex
}

func (self *TaskWorker) itemKey(taskId, item string) string {
	return *self.config.Etcd.KeyPrefix + "/" + item + "/" + taskId
}

func (self *TaskWorker) tryAddTask(task *_TaskInfo) bool {
	// 尝试创建任务的key Owner，如果创建成功则认为获取到任务
	resp, err := self.etcd.Client.Grant(context.TODO(), *self.config.TaskOwnTime)
	if err != nil {
		log.Errorf("[%s] etcd.Client.Grant got error:%s, TaskOwnTime:%d", task.param.TaskId, err.Error(),
			self.config.TaskOwnTime)
		return false
	}
	opts := []clientv3.OpOption{clientv3.WithLease(resp.ID)}
	keyParam := self.itemKey(task.param.TaskId, "TaskParam")
	cmpParam := clientv3.Compare(clientv3.CreateRevision(keyParam), "!=", 0)
	keyOwner := self.itemKey(task.param.TaskId, "Owner")
	cmpOwner := self.etcd.CmpKeyNotExist(keyOwner)
	opOwner := clientv3.OpPut(keyOwner, fmt.Sprintf(`{"instance": "%s"}`, self.config.InstanceId), opts...)
	opFetch := clientv3.OpDelete(self.itemKey(task.param.TaskId, "Fetch/"+task.param.TaskType))
	cmps := []clientv3.Cmp{cmpParam, cmpOwner} // task_param存在，owner不存在
	ifs := []clientv3.Op{opOwner, opFetch}
	respt, err := self.etcd.Txn(cmps, ifs, nil)
	if err != nil {
		log.Infof("[%s] etcd.Txn got error:%s", task.param.TaskId, err.Error())
		return false
	}
	if !respt.Succeeded {
		log.Infof("[%s] FAILED!", task.param.TaskId)
		return false
	}
	self.mutex.Lock()
	self.taskProcessing[task.param.TaskId] = task
	self.mutex.Unlock()
	log.Infof("[%s] take task owner succeed", task.param.TaskId)
	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "add_task", "task_id": task.param.TaskId, "task_type": task.param.TaskType})

	// 如果已有Status key则读取
	vals, err := self.etcd.Get(self.itemKey(task.param.TaskId, "Status"), false)
	if err == nil && len(vals) > 0 {
		if err = json.Unmarshal(vals[0], &task.status); err != nil {
			log.Errorf("[%s] json.Unmarshal got error [%s] on parse existing status val [%s]",
				task.param.TaskId, err.Error(), string(vals[0]))
		} else {
			log.Debugf("[%s] parse existing status ok [%#v]", task.param.TaskId, task.status)
		}
	} else {
		log.Debugf("[%s] no existing status", task.param.TaskId)
	}
	task.status.StartTime = time.Now().Unix()
	self.updateStatusValue(task, TaskStatusInit, []byte(""))

	// 起一个goroutine监控任务的Owner key，被删掉时停止任务
	on_delete := func(key string, val []byte) bool {
		self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
			"reason": "owner key deleted", "status": fmt.Sprintf("%#v", task.status)})
		self.config.CbTaskStop(&task.param)
		self.chRemove <- task
		return true
	}
	ctxCancel, cancel := context.WithCancel(context.Background())
	task.cancelOwnerWatch = cancel
	go self.etcd.WatchCallback(self.itemKey(task.param.TaskId, "Owner"), "DELETE", true, on_delete, ctxCancel)

	// 起一个goroutine监控任务参数修改
	if self.config.CbTaskModify != nil {
		on_put := func(key string, val []byte) bool {
			if bytes.Compare(val, task.param.UserParam) == 0 {
				self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId,
					"result": "no change, skipped"})
				return true
			}

			if err := json.Unmarshal(val, &task.param); err != nil {
				self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId,
					"result": fmt.Sprintf("json.Unmarshal error [%s], key [%s], val [%s]", err.Error(), key, string(val))})
				return true
			}
			self.config.CbTaskModify(&task.param)
			self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId,
				"result": "processed"})
			return true
		}
		ctxCancel, cancel := context.WithCancel(context.Background())
		task.cancelParamWatch = cancel
		go self.etcd.WatchCallback(self.itemKey(task.param.TaskId, "TaskParam"), "PUT", true, on_put, ctxCancel)
	}

	// 起一个goroutine做keepalive
	task.leaseId = resp.ID
	task.rentTime = time.Now().Unix()
	go func() {
		log.Infof("[%s] start keepalive...", task.param.TaskId)
		ticker := time.NewTicker(time.Second * 1)
		tickChan := ticker.C
		for {
			select {
			case <-tickChan:
				if time.Now().Unix()-task.rentTime > *self.config.TaskOwnTime*3/4 {
					resp, err := self.etcd.Client.KeepAliveOnce(context.TODO(), task.leaseId)
					if err != nil {
						self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
							"reason": "auto keepalive got error:" + err.Error()})
						self.chRemove <- task
					} else {
						log.Debugf("[%s] auto keepalive ok, ttl:%d", task.param.TaskId, resp.TTL)
						task.rentTime = time.Now().Unix()
					}
				}
			case <-ctxCancel.Done():
				log.Infof("[%s] keepalive goroutine got cancel, exit", task.param.TaskId)
				ticker.Stop()
				return
			}
		}
	}()

	// 启动任务
	err = self.config.CbTaskStart(&task.param)
	if err == nil {
		log.Debugf("CbTaskStart ret Ok, set status to [%s]", TaskStatusInit)
	} else {
		self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
			"reason": "CbTaskStart ret error:" + err.Error(), "status": fmt.Sprintf("%#v", task.status)})
		self.updateStatusValue(task, TaskStatusError, []byte(""))
		self.removeTask(task)
		return false
	}

	log.Debugf("[%s] start task complete", task.param.TaskId)
	return true
}

func (self *TaskWorker) removeTask(task *_TaskInfo) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	t, ok := self.taskProcessing[task.param.TaskId]
	if !ok {
		log.Criticalf("[%s] task not found, taskType:%s", task.param.TaskId, task.param.TaskType)
		return false
	}
	delete(self.taskProcessing, task.param.TaskId)

	log.Infof("[%s] task remove, remove from queue", task.param.TaskId)
	go func(task *_TaskInfo) {
		log.Debugf("[%s] task remove, call CbTaskStop", task.param.TaskId)
		self.config.CbTaskStop(&task.param)
		log.Debugf("[%s] task remove, call cancelOwnerWatch", task.param.TaskId)
		task.cancelOwnerWatch()
		log.Debugf("[%s] task remove, call cancelParamWatch", task.param.TaskId)
		task.cancelParamWatch()
		log.Debugf("[%s] task remove, Del owner", task.param.TaskId)
		self.etcd.Del(self.itemKey(task.param.TaskId, "Owner"), false)
		log.Infof("[%s] task remove complete", task.param.TaskId)
	}(t)
	return true
}

func (self *TaskWorker) checkNewTasks() {
	for taskType, queue := range self.taskQueue {
		for task := queue.Front(); task != nil; {
			t := task
			task = task.Next()
			taskInfo := t.Value.(*_TaskInfo)
			if time.Now().Unix()-taskInfo.foundTime > *self.config.MaxQueueTime {
				log.Debugf("[%s][%s] remove timeout from queue", taskInfo.param.TaskId, taskType)
				queue.Remove(t)
			} else {
				err := self.config.CbTaskAddCheck(&taskInfo.param)
				if err == nil {
					log.Debugf("[%s][%s] try take owner", taskInfo.param.TaskId, taskType)
					if !self.tryAddTask(taskInfo) {
						log.Debugf("[%s][%s] take owner FAILED! skip", taskInfo.param.TaskId, taskType)
					}
					queue.Remove(t)
				} else if err == ErrNotSupport {
					log.Debugf("[%s][%s] CbTaskAddCheck ret [%s], skip", taskInfo.param.TaskId, taskType, err.Error())
					queue.Remove(t)
				} else if err == ErrOutOfResource {
					taskInfo.lastOORTime = time.Now().UnixNano()
					log.Debugf("[%s][%s] CbTaskAddCheck ret [%s], pause check", taskInfo.param.TaskId, taskType, err.Error())
					break
				} else {
					log.Criticalf("CbTaskAddCheck invalid return value [%v], client code error", err)
					queue.Remove(t)
				}
			}
		}
	}
}

func (self *TaskWorker) updateStatusValue(task *_TaskInfo, status string, userParam []byte) {
	task.status.Status = status
	task.status.UserParam = userParam
	task.status.UpdateTime = time.Now().Unix()
	statusValue, _ := json.Marshal(task.status)
	self.etcd.Put(self.itemKey(task.param.TaskId, "Status"), string(statusValue), 0)

	if !task.errorCleaned && time.Now().Unix()-task.status.StartTime > 100 {
		log.Warnf("[%s] task run ok, clean error_info", task.param.TaskId)
		self.etcd.Del(self.itemKey(task.param.TaskId, "ErrorInfo"), false)
		task.errorCleaned = true
	}
}

type FetchVisitor struct {
	caller string
	opts   []clientv3.OpOption
	chTask chan *_TaskInfo
}

func (self *FetchVisitor) Visit(key string, val []byte) bool {
	// /Fetch/{TaskType}/{taskId}
	secs := strings.Split(key, "/")
	if len(secs) < 2 {
		log.Errorf("invalid key, caller [%s], key [%s], val [%s]", self.caller, key, string(val))
		return true
	}

	task := _TaskInfo{}
	if err := json.Unmarshal(val, &task.param); err != nil {
		log.Errorf("json.Unmarshal error [%s], , caller [%s], key [%s], val [%s]", err.Error(), self.caller, key, string(val))
		return true
	}

	task.foundTime = time.Now().Unix()
	task.param.TaskId = secs[len(secs)-1]
	task.param.TaskType = secs[len(secs)-2]
	log.Infof("[%s] found new task, id [%s], type [%s]", self.caller, task.param.TaskId, task.param.TaskType)

	self.chTask <- &task
	return true
}

func (self *TaskWorker) watchNewTask() {
	visitor := FetchVisitor{}
	visitor.caller = "watchNewTask"
	visitor.chTask = self.chTask
	for _, taskType := range *self.config.TaskTypes {
		go self.etcd.WatchVisitor(*self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, "PUT", true, &visitor, nil)
	}
}

func (self *TaskWorker) scanExistingTasks() {
	visitor := FetchVisitor{}
	visitor.caller = "scanExistingTasks"
	visitor.chTask = self.chTask
	visitor.opts = []clientv3.OpOption{}
	visitor.opts = append(visitor.opts, clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend))
	for _, taskType := range *self.config.TaskTypes {
		go self.etcd.WalkVisitor(*self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, &visitor, -1, nil)
	}
}

func (self *TaskWorker) Init(config *TaskWorkerConfig) error {
	self.config = *config
	err := self.etcd.Init(*config.Etcd.Endpoints, *config.Etcd.DialTimeout)
	if err != nil {
		log.Criticalf("etcd.Init got err [%s], Endpoints [%#v], DialTimeout [%d]",
			err.Error(), config.Etcd.Endpoints, config.Etcd.DialTimeout)
		return err
	}

	self.chTask = make(chan *_TaskInfo, 1000)
	self.chRemove = make(chan *_TaskInfo, 1000)

	self.taskQueue = make(map[string]*list.List)
	for _, taskType := range *self.config.TaskTypes {
		self.taskQueue[taskType] = list.New()
	}
	self.taskProcessing = make(map[string]*_TaskInfo)

	log.Infof("Init OK")
	return nil
}

func (self *TaskWorker) Start(exitKeepAliveFail bool) {
	go func() {
		self.watchNewTask()
		self.scanExistingTasks()
		tickChan := time.NewTicker(time.Millisecond * 100).C
		for {
			select {
			case task := <-self.chTask:
				self.taskQueue[task.param.TaskType].PushBack(task)
			case task := <-self.chRemove:
				self.removeTask(task)
			case <-tickChan:
				self.checkNewTasks()
			}
		}
	}()

	if exitKeepAliveFail {
		go func() {
			ch := make(chan string, 100)
			onKeepAlive := func(key string, val []byte) bool {
				msg := string(val)
				ch <- msg
				return true
			}
			go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/KeepAlive", "PUT", true, onKeepAlive, nil)

			tickChan := time.NewTicker(time.Second * 60).C
			lastUpdate := time.Now().Unix()
			for {
				select {
				case <-ch:
					lastUpdate = time.Now().Unix()
				case <-tickChan:
					// mdp-manager每60s更新一次/KeepAlive
					if time.Now().Unix()-lastUpdate > 60*5 {
						self.config.CbLogJson(log.LevelCritical, log.Json{"cmd": "keepalive_failed",
							"now": time.Now().Unix(), "last_update": lastUpdate})
						self.etcd.Exit()
						os.Exit(0)
					}
				}
			}
		}()
	}

	log.Infof("Start OK")
}

func (self *TaskWorker) UpdateTaskStatus(param *TaskParam, status string, userParam []byte) {
	self.mutex.Lock()
	taskInfo, ok := self.taskProcessing[param.TaskId]
	self.mutex.Unlock()
	if !ok {
		log.Warnf("[%s][%s] task not found, status [%s]", param.TaskId, param.TaskType, status)
		return
	}
	self.updateStatusValue(taskInfo, status, userParam)
	if status == TaskStatusComplete || status == TaskStatusError || status == TaskStatusRestart {
		preTime := taskInfo.status.StartTime - param.AddTime
		processingTime := time.Now().Unix() - taskInfo.status.StartTime
		self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": param.TaskId,
			"pre_time": preTime, "processing_time": processingTime, "status_str": status,
			"reason": "status error or complete", "status": fmt.Sprintf("%#v", taskInfo.status)})
		self.chRemove <- taskInfo
	}
}
