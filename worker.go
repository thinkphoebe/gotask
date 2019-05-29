package gotask

import "C"
import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	log "github.com/thinkphoebe/golog"
	"github.com/thinkphoebe/goetcd"
)

type _TaskInfo struct {
	param            TaskParam
	status           TaskStatus
	foundTime        int64
	rentTime         int64
	leaseId          clientv3.LeaseID
	cancelOwnerWatch context.CancelFunc
	errorCleaned     bool
}

type TaskWorker struct {
	config WorkerConfig

	etcd     goetcd.Etcd
	chTask   chan *_TaskInfo
	chRemove chan *_TaskInfo

	// watch到的任务队列，每taskType一个队列
	taskQueue map[string]*list.List

	// 已获取Owner正在处理的任务的信息
	taskProcessing map[string]*_TaskInfo
}

func (self *TaskWorker) itemKey(taskId, item string) string {
	return self.config.Etcd.KeyPrefix + "/" + item + "/" + taskId
}

func (self *TaskWorker) tryAddTask(task *_TaskInfo) bool {
	// 尝试创建任务的key Owner，如果创建成功则认为获取到任务
	resp, err := self.etcd.Client.Grant(context.TODO(), self.config.TaskOwnTime)
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
	opOwner := clientv3.OpPut(keyOwner, fmt.Sprintf(`{"instance": "%s"}`, self.config.GInstanceId), opts...)
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
	log.Infof("[%s] take task owner succeed, config [%#v]", task.param.TaskId, task.param)
	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "add_task", "task_id": task.param.TaskId,
		"task_type": task.param.TaskType, "user_param": task.param.UserParam})

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

	// 启动任务
	task.status.StartTime = time.Now().Unix()
	err = self.config.CbTaskStart(&task.param)
	if err == nil {
		self.updateStatusValue(task, TaskStatusInit, "")
		log.Debugf("CbTaskStart ret Ok, set status to [%s]", TaskStatusInit)
	} else {
		self.updateStatusValue(task, "error", "")
		self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
			"reason": "CbTaskStart ret error:" + err.Error(), "status": fmt.Sprintf("%#v", task.status)})
		self.etcd.Del(keyOwner, false)
		return false
	}

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

	task.rentTime = time.Now().Unix()
	task.leaseId = resp.ID
	log.Debugf("[%s] start task complete", task.param.TaskId)
	return true
}

func (self *TaskWorker) removeTask(task *_TaskInfo) bool {
	t, ok := self.taskProcessing[task.param.TaskId]
	if !ok {
		log.Criticalf("[%s] task not found, taskType:%s", task.param.TaskId, task.param.TaskType)
		return false
	}

	log.Infof("[%s] task remove, remove from queue", task.param.TaskId)
	go func(task *_TaskInfo) {
		log.Debugf("[%s] task remove, call CbTaskStop", task.param.TaskId)
		self.config.CbTaskStop(&task.param)
		log.Debugf("[%s] task remove, call cancelOwnerWatch", task.param.TaskId)
		task.cancelOwnerWatch()
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
			if time.Now().Unix()-taskInfo.foundTime > self.config.MaxQueueTime {
				log.Debugf("[%s][%s] remove timeout from queue", taskInfo.param.TaskId, taskType)
				queue.Remove(t)
			} else {
				err := self.config.CbTaskAddCheck(&taskInfo.param)
				if err == nil {
					log.Debugf("[%s][%s] try take owner", taskInfo.param.TaskId, taskType)
					if self.tryAddTask(taskInfo) {
						self.taskProcessing[taskInfo.param.TaskId] = taskInfo
					}
				} else if err == ErrNotSupport {
					log.Debugf("[%s][%s] CbTaskAddCheck ret [%s], skip", taskInfo.param.TaskId, taskType, err.Error())
					queue.Remove(t)
				} else if err == ErrOutOfResource {
					log.Debugf("[%s][%s] CbTaskAddCheck ret [%s], pause check", taskInfo.param.TaskId, taskType, err.Error())
					break
				} else {
					log.Criticalf("CbTaskAddCheck invalid return value [%v], client code error", err)
				}
			}
		}
	}
}

func (self *TaskWorker) updateStatusValue(task *_TaskInfo, status string, userParam string) {
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
	for _, taskType := range self.config.TaskTypes {
		go self.etcd.WatchVisitor(self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, "PUT", true, &visitor, nil)
	}
}

func (self *TaskWorker) scanExistingTasks() {
	visitor := FetchVisitor{}
	visitor.caller = "scanExistingTasks"
	visitor.chTask = self.chTask
	visitor.opts = []clientv3.OpOption{}
	visitor.opts = append(visitor.opts, clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend))
	for _, taskType := range self.config.TaskTypes {
		go self.etcd.WalkVisitor(self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, &visitor, -1, nil)
	}
}

func (self *TaskWorker) Init(config *WorkerConfig) error {
	self.config = *config
	err := self.etcd.Init(config.Etcd.Endpoints, config.Etcd.DialTimeout)
	if err != nil {
		log.Criticalf("etcd.Init got err [%s], Endpoints [%#v], DialTimeout [%d]",
			err.Error(), config.Etcd.Endpoints, config.Etcd.DialTimeout)
		return err
	}

	self.chTask = make(chan *_TaskInfo, 1000)
	self.chRemove = make(chan *_TaskInfo, 1000)

	self.taskQueue = make(map[string]*list.List)
	for _, taskType := range self.config.TaskTypes {
		self.taskQueue[taskType] = list.New()
	}
	self.taskProcessing = make(map[string]*_TaskInfo)

	log.Infof("Init OK")
	return nil
}

func (self *TaskWorker) Start() {
	go func() {
		self.watchNewTask()
		self.scanExistingTasks()

		for {
			select {
			case task := <-self.chTask:
				self.taskQueue[task.param.TaskType].PushBack(task)
			case task := <-self.chRemove:
				self.removeTask(task)
			default:
				self.checkNewTasks()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	log.Infof("Start OK")
}

func (self *TaskWorker) UpdateTaskStatus(param *TaskParam, status, userParam string) {
	taskInfo, ok := self.taskProcessing[param.TaskId]
	if !ok {
		log.Warnf("[%s] task not found", param.TaskId)
		return
	}

	self.updateStatusValue(taskInfo, status, userParam)
	if status == TaskStatusWorking {
		resp, err := self.etcd.Client.KeepAliveOnce(context.TODO(), taskInfo.leaseId)
		if err != nil {
			self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": param.TaskId,
				"reason": "keepalive got error:" + err.Error(), "status": fmt.Sprintf("%#v", taskInfo.status)})
			self.chRemove <- taskInfo
		} else {
			log.Debugf("[%s] keepalive ok, ttl:%d", taskInfo.param.TaskId, resp.TTL)
			taskInfo.rentTime = time.Now().Unix()
		}
	} else if status == TaskStatusComplete || status == TaskStatusError {
		self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": param.TaskId,
			"reason": "status error or complete", "status": fmt.Sprintf("%#v", taskInfo.status)})
		self.chRemove <- taskInfo
	}
}
