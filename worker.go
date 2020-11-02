package gotask

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/thinkphoebe/goetcd"
	log "github.com/thinkphoebe/golog"
)

// 任务处理流程：visitor -> chTask -> send2Queues -> checkNewTasks -> tryAddTask
// priority值越小，优先级越高
// 假设一个WaitResourceTask的priority值为a，处理时优先级介于a-1和a之间，或认为排在相同priority的任务之前

// WaitResource不利于资源充分利用，因为Wait的任务一般需要大块资源，而为Wait任务保留但还不满足Wait任务执行的资源无法被其它任务使用

type _ResourceInfo struct {
	total   int
	used    int
	reserve int
}

type _PriorityQueue struct {
	first        *list.List // 多次被dispatch的任务，优先检查
	second       *list.List
	waitResource *_TaskInfo // wait resource只对当前的priority有效，wait期间如有更高priority的任务资源会被分走
	mutex        sync.Mutex
}

type _TaskInfo struct {
	param            TaskParam
	status           TaskStatus
	resource         map[string]*TaskResource
	foundTime        int64
	startTime        int64 // 目前除了日志打印没有其它用途
	rentTime         int64
	leaseId          clientv3.LeaseID
	cancelOwnerWatch context.CancelFunc
	cancelParamWatch context.CancelFunc // 两处使用：WaitResource时监控任务的删除；执行过程中监控参数修改
	errorCleaned     bool
}

type TaskWorker struct {
	config TaskWorkerConfig

	etcd      goetcd.Etcd
	chTask    chan *_TaskInfo
	chRemove  chan *_TaskInfo
	stopFetch bool

	lastResourceUpdate int64
	resources          map[string]*_ResourceInfo // key: resource name

	// watch到的任务队列，key: priority
	taskQueues map[int]*_PriorityQueue
	// 辅助queues按照priority排序
	priorities []int

	// 已获取Owner正在处理的任务的信息。会在UpdateTaskStatus()被Client调用，所以加锁
	taskProcessing map[string]*_TaskInfo
	mutex          sync.Mutex
}

func (self *TaskWorker) itemKey(taskId, item string) string {
	return *self.config.Etcd.KeyPrefix + "/" + item + "/" + taskId
}

func (self *TaskWorker) logJson(level log.LogLevel, j log.Json) {
	self.config.CbLogJson(self.config.InstanceHandle, level, j)
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
	self.logJson(log.LevelInfo, log.Json{"cmd": "add_task", "task_id": task.param.TaskId, "task_type": task.param.TaskType})

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
		self.logJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
			"reason": "owner key deleted", "status": fmt.Sprintf("%#v", task.status)})
		self.chRemove <- task
		return true
	}
	ctxCancel, cancel := context.WithCancel(context.Background())
	task.cancelOwnerWatch = cancel
	go self.etcd.WatchCallback(self.itemKey(task.param.TaskId, "Owner"), "DELETE", false, on_delete, ctxCancel)

	// 起一个goroutine监控任务参数修改
	if self.config.CbTaskModify != nil {
		on_put := func(key string, val []byte) bool {
			if bytes.Compare(val, task.param.UserParam) == 0 {
				self.logJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId,
					"result": "no change, skipped"})
				return true
			}

			if err := json.Unmarshal(val, &task.param); err != nil {
				self.logJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId,
					"result": fmt.Sprintf("json.Unmarshal error [%s], key [%s], val [%s]", err.Error(), key, string(val))})
				return true
			}
			self.config.CbTaskModify(self.config.InstanceHandle, &task.param)
			self.logJson(log.LevelInfo, log.Json{"cmd": "modify_task", "task_id": task.param.TaskId, "result": "processed"})
			return true
		}
		ctxCancel, cancel := context.WithCancel(context.Background())
		task.cancelParamWatch = cancel
		go self.etcd.WatchCallback(self.itemKey(task.param.TaskId, "TaskParam"), "PUT", false, on_put, ctxCancel)
	}

	// 起一个goroutine做keepalive
	task.leaseId = resp.ID
	task.startTime = time.Now().Unix()
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
						self.logJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
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
	err = self.config.CbTaskStart(self.config.InstanceHandle, &task.param)
	if err == nil {
		log.Debugf("CbTaskStart ret Ok, set status to [%s]", TaskStatusInit)
	} else {
		self.logJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": task.param.TaskId,
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
		self.config.CbTaskStop(self.config.InstanceHandle, &task.param)
		log.Debugf("[%s] task remove, call cancelOwnerWatch", task.param.TaskId)
		task.cancelOwnerWatch()
		if task.cancelParamWatch != nil {
			log.Debugf("[%s] task remove, call cancelParamWatch", task.param.TaskId)
			task.cancelParamWatch()
		}
		log.Debugf("[%s] task remove, delete owner", task.param.TaskId)
		self.etcd.Del(self.itemKey(task.param.TaskId, "Owner"), false)
		log.Infof("[%s] task remove complete", task.param.TaskId)
	}(t)
	return true
}

func (self *TaskWorker) checkNewTasks() {
	updateResource := func() {
		if self.config.Resources == nil || self.config.ResourceUpdateInterval == nil ||
				*self.config.ResourceUpdateInterval <= 0 || self.config.CbGetResourceInfo == nil {
			return
		}
		if time.Now().Unix()-self.lastResourceUpdate < *self.config.ResourceUpdateInterval {
			return
		}
		self.lastResourceUpdate = time.Now().Unix()
		for _, info := range *self.config.Resources {
			total, used, err := self.config.CbGetResourceInfo(self.config.InstanceHandle, info.Name)
			if err != nil {
				log.Errorf("[updateResource] CbGetResourceInfo [%s] ret err:%v", info.Name, err)
				continue
			}
			res := self.resources[info.Name]
			res.total = total
			res.used = used
			log.Debugf("[updateResource][%s] total:%d, reserve:%d, used:%d",
				info.Name, total, info.Reserve, used)
		}
	}

	doWaitResource := func(queue *_PriorityQueue, taskInfo *_TaskInfo) bool {
		queue.mutex.Lock()
		if queue.waitResource != nil {
			log.Debugf("[WaitResource][%s][%s] already have wait resource task [%s], skip wait",
				taskInfo.param.TaskId, taskInfo.param.TaskType, queue.waitResource.param.TaskId)
			queue.mutex.Unlock()
			return false
		}
		queue.mutex.Unlock()

		key := self.itemKey(taskInfo.param.TaskId+"/"+self.config.InstanceId, "WaitResource")
		cmp := clientv3.Compare(clientv3.CreateRevision(key), "==", 0)
		opPut, _ := self.etcd.OpPut(key, "", 0)
		opGet := self.etcd.OpGet(key, false)
		resp, err := self.etcd.Txn([]clientv3.Cmp{cmp}, []clientv3.Op{*opPut}, []clientv3.Op{*opGet})
		var rev int64
		if err != nil {
			log.Errorf("[WaitResource][%s] etcd.Txn got err:%v", taskInfo.param.TaskId, err)
			return false
		}
		if resp.Succeeded {
			rev = resp.Responses[0].GetResponsePut().Header.Revision
			log.Debugf("[WaitResource][%s] create key [%s] succeed, revision:%d", key, rev)
		} else {
			rev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
			log.Debugf("[WaitResource][%s] key [%s] already exists, revision:%d", key, rev)
		}

		getOpts := append(clientv3.WithLastCreate(), clientv3.WithMaxCreateRev(rev-1))
		ctx, _ := context.WithTimeout(context.TODO(), *self.config.Etcd.DialTimeout*time.Second)
		r, err := self.etcd.Client.Get(ctx, self.itemKey(taskInfo.param.TaskId, "WaitResource"), getOpts...)
		if err == nil && len(r.Kvs) >= *self.config.MaxWaitResourceWorkers {
			log.Infof("[WaitResource][%s] already have %d waiting workers", taskInfo.param.TaskId, len(r.Kvs))
			self.etcd.Del(key, false)
			return false
		}
		queue.mutex.Lock()
		queue.waitResource = taskInfo
		queue.mutex.Unlock()

		// 起一个goroutine监控TaskParam, WaitResource的任务被删除时停止wait
		on_delete := func(key string, val []byte) bool {
			queue.mutex.Lock()
			queue.waitResource = nil
			queue.mutex.Unlock()
			self.logJson(log.LevelInfo, log.Json{"cmd": "wait_resource_removed", "task_id": taskInfo.param.TaskId,
				"task_type": taskInfo.param.TaskType, "reason": "TaskParam deleted"})
			return true
		}
		ctxCancel, cancel := context.WithCancel(context.Background())
		taskInfo.cancelParamWatch = cancel
		go self.etcd.WatchCallback(key, "DELETE", false, on_delete, ctxCancel)

		self.logJson(log.LevelInfo, log.Json{"cmd": "wait_resource_added",
			"task_id": taskInfo.param.TaskId, "task_type": taskInfo.param.TaskType})
		return true
	}

	checkResource := func(taskInfo *_TaskInfo, taskPriority int, isWaitResourceTask bool) (bool, map[string]bool) {
		var err error
		var checkInfo = make(map[string]bool)
		var succeed = true

		if self.config.CbTaskAddCheck != nil && !self.config.CbTaskAddCheck(&taskInfo.param) {
			log.Debugf("[checkResource][%s] config.CbTaskAddCheck ret false", taskInfo.param.TaskId)
			return false, checkInfo
		}
		if self.config.CbGetTaskResources == nil {
			log.Debugf("[checkResource][%s] config.CbGetTaskResources is nil, ret true", taskInfo.param.TaskId)
			return true, checkInfo
		}
		taskInfo.resource, err = self.config.CbGetTaskResources(self.config.InstanceHandle, &taskInfo.param)
		if err != nil {
			log.Debugf("[checkResource][%s] config.CbGetTaskResources got err [%v], ret false",
				taskInfo.param.TaskId, err)
			return false, checkInfo
		}

		for resType, resInfo := range taskInfo.resource {
			sysRes, ok := self.resources[resType]
			if !ok {
				log.Criticalf("[checkResource][%s] CbGetTaskResources return an unknown resType [%d], bug in user code, ret false",
					taskInfo.param.TaskId, resType)
				return false, checkInfo
			}

			// 所有处理中任务占用的资源
			usedProc := 0
			for _, task := range self.taskProcessing {
				if r, ok := task.resource[resType]; ok {
					if time.Now().Unix() > task.status.StartTime+int64(r.AllocateTime) {
						usedProc += r.Reserve
					} else {
						usedProc += r.Need
					}
				}
			}

			// 所有已在队列中且优先级更高的任务需要的资源
			// 普通任务将同优先级的任务计算进去，因为普通任务优先级相同时应先到先处理
			// WaitResourceTask不包括同优先级任务，因为WaitResourceTask会在同优先级任务之前处理
			// 由于以下总是按priority从小到达遍历，实际上usedQueue应该永远为0？
			usedQueue := 0
			for _, priority := range self.priorities { // 按照priority从小到大遍历
				if priority > taskPriority || priority == taskPriority && isWaitResourceTask {
					break
				}
				queue := self.taskQueues[priority]
				queue.mutex.Lock()
				if queue.waitResource != nil {
					if r, ok := queue.waitResource.resource[resType]; ok {
						usedQueue += r.Need
					}
				}
				queue.mutex.Unlock()
			}

			log.Debugf("[checkResource][%s] resType [%s], sys total:%d, sys reserve:%d, sys used:%d, processing used:%d, queue need:%d, task need:%d",
				taskInfo.param.TaskId, resType, sysRes.total, sysRes.reserve, sysRes.used, usedProc, usedQueue, resInfo.Need)
			if usedProc+usedQueue+resInfo.Need > sysRes.total-sysRes.used-sysRes.reserve {
				log.Debugf("[checkResource][%s] resource [%s] not enough", taskInfo.param.TaskId, resType)
				succeed = false
				checkInfo[resType] = false
			} else {
				checkInfo[resType] = true
			}
		}
		log.Debugf("[checkResource][%s] check completed, succeed:%v", taskInfo.param.TaskId, succeed)
		return succeed, checkInfo
	}

	writeResourceInfo := func(taskInfo *_TaskInfo, succeed bool, checkInfo map[string]bool) {
		resInfo := WorkerResourceCheck{
			TaskId:          taskInfo.param.TaskId,
			AddTime:         taskInfo.param.AddTime,
			ResourceDetails: checkInfo,
			Succeed:         succeed,
		}
		paramBytes, _ := json.Marshal(resInfo)
		keyInfo := *self.config.Etcd.KeyPrefix + "/WorkerResourceCheck/" + strconv.FormatInt(time.Now().UnixNano(), 10)
		self.etcd.Put(keyInfo, string(paramBytes), 3600)
	}

	updateResource()
	for _, priority := range self.priorities { // 按照priority从小到大遍历
		queue := self.taskQueues[priority]

		queue.mutex.Lock()
		waitResource := queue.waitResource
		queue.mutex.Unlock()
		if waitResource != nil {
			log.Debugf("priority [%d] has waitResourceTask, check first", priority)
			succeed, checkInfo := checkResource(waitResource, priority, true)
			if succeed {
				log.Debugf("[%s] waitResourceTask have resource now, try to add", waitResource.param.TaskId)
				// 成功后删除WaitResource、FailedResource信息
				key := self.itemKey(waitResource.param.TaskId+"/"+self.config.InstanceId, "WaitResource")
				self.etcd.Del(key, false)
				for resType := range checkInfo {
					key = *self.config.Etcd.KeyPrefix + "/FailedResource/" + resType + "/" + waitResource.param.TaskId
					self.etcd.Del(key, false)
				}

				waitResource.cancelParamWatch()
				waitResource.cancelParamWatch = nil
				queue.mutex.Lock()
				queue.waitResource = nil
				queue.mutex.Unlock()
				startOk := false
				if self.tryAddTask(waitResource) {
					// WaitResource的任务获取资源成功时不写ResourceInfo
					startOk = true
				}
				self.logJson(log.LevelInfo, log.Json{"cmd": "wait_resource_removed",
					"task_id": waitResource.param.TaskId, "task_type": waitResource.param.TaskType,
					"reason": "resource satisfied", "start_ok": startOk})
			} else {
				log.Debugf("[%s] wait resource task checkResource FAILED!", waitResource.param.TaskId)
				writeResourceInfo(waitResource, succeed, checkInfo)
			}
		}

		for task := queue.first.Front(); task != nil; {
			t := task
			task = task.Next()
			taskInfo := t.Value.(*_TaskInfo)
			queue.first.Remove(t)

			succeed, checkInfo := checkResource(taskInfo, priority, false)
			if succeed {
				log.Debugf("[%s] checkResource from first queue ok, try to add", taskInfo.param.TaskId)
				if self.tryAddTask(taskInfo) {
					// checkResource成功时，只有tryAddTask成功的才写，便于统计SucceedTaskCount
					writeResourceInfo(taskInfo, succeed, checkInfo)
				}
			} else {
				log.Debugf("[%s] checkResource from first queue FAILED! wait resource:%d",
					taskInfo.param.TaskId, taskInfo.param.WaitResource)
				if taskInfo.param.WaitResource != 0 {
					doWaitResource(queue, taskInfo)
				}
				writeResourceInfo(taskInfo, succeed, checkInfo)
			}
		}

		for task := queue.second.Front(); task != nil; {
			t := task
			task = task.Next()
			taskInfo := t.Value.(*_TaskInfo)
			queue.second.Remove(t)

			// 跳过在队列里过长时间的任务，很可能已被其它worker取走，即使未被取走manager也会重新分发
			if time.Now().Unix()-taskInfo.foundTime > *self.config.MaxQueueTime {
				log.Debugf("[%s][%s] remove timeout from queue", taskInfo.param.TaskId, taskInfo.param.TaskType)
				continue
			}

			succeed, checkInfo := checkResource(taskInfo, priority, false)
			if succeed {
				log.Debugf("[%s] checkResource from second queue ok, try to add", taskInfo.param.TaskId)
				if self.tryAddTask(taskInfo) {
					writeResourceInfo(taskInfo, succeed, checkInfo)
				}
			} else {
				log.Debugf("[%s] checkResource from second queue FAILED, skip", taskInfo.param.TaskId)
				writeResourceInfo(taskInfo, succeed, checkInfo)
			}
		}
	}
}

func (self *TaskWorker) send2Queues(task *_TaskInfo) {
	priority, exist := (*self.config.TaskTypes)[task.param.TaskType]
	if !exist {
		self.logJson(log.LevelInfo, log.Json{"cmd": "unknown_task", "task_id": task.param.TaskId,
			"task_type": task.param.TaskType})
		return
	}

	q := self.taskQueues[priority]
	if task.param.DispatchCount > *self.config.DispatchThreshold {
		// 按从大到小的顺序插入
		inserted := false
		pos := 0
		for t := q.first.Front(); t != nil; t = t.Next() {
			if task.param.DispatchCount > t.Value.(*_TaskInfo).param.DispatchCount {
				log.Debugf("[%s] insert to first queue, pos:%d", task.param.TaskId, pos)
				q.first.InsertBefore(task, t)
				inserted = true
				break
			}
			pos++
		}
		if !inserted {
			log.Debugf("[%s] put to end of first queue, pos:%d", task.param.TaskId, pos)
			q.first.PushBack(task)
		}
	} else {
		log.Debugf("[%s] put to second queue, pos:%d", task.param.TaskId, q.second.Len())
		q.second.PushBack(task)
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
	log.Infof("[%s] found new task, id [%s], type [%s], dispatch count [%d]",
		self.caller, task.param.TaskId, task.param.TaskType, task.param.DispatchCount)
	self.chTask <- &task
	return true
}

func (self *TaskWorker) watchNewTask() {
	visitor := FetchVisitor{}
	visitor.caller = "watchNewTask"
	visitor.chTask = self.chTask
	for taskType := range *self.config.TaskTypes {
		go self.etcd.WatchVisitor(*self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, "PUT", true, &visitor, nil)
	}
}

func (self *TaskWorker) scanExistingTasks() {
	visitor := FetchVisitor{}
	visitor.caller = "scanExistingTasks"
	visitor.chTask = self.chTask
	for taskType := range *self.config.TaskTypes {
		go self.etcd.WalkVisitor(*self.config.Etcd.KeyPrefix+"/Fetch/"+taskType, &visitor, -1, 500, nil)
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

	self.resources = make(map[string]*_ResourceInfo)
	if config.Resources != nil && config.CbGetResourceInfo != nil {
		for name, info := range *config.Resources {
			total, used, err := config.CbGetResourceInfo(config.InstanceHandle, name)
			log.Infof("new resource type [%s]. used:%d, err:%v", name, used, err)
			self.resources[name] = &_ResourceInfo{total: total, used: used, reserve: info.Reserve}
		}
	}

	self.taskProcessing = make(map[string]*_TaskInfo)

	self.taskQueues = make(map[int]*_PriorityQueue)
	self.priorities = make([]int, 0)
	for _, priority := range *self.config.TaskTypes {
		self.taskQueues[priority] = &_PriorityQueue{first: list.New(), second: list.New()}
		self.priorities = append(self.priorities, priority)
	}
	sort.Ints(self.priorities)

	log.Infof("Init OK")
	return nil
}

func (self *TaskWorker) Start(exitKeepAliveFail bool) {
	printStatus := func() {
		self.mutex.Lock()
		log.Infof("stop fetch:%v, tasks in processing:%d", self.stopFetch, len(self.taskProcessing))
		for id, info := range self.taskProcessing {
			log.Debugf("[%s] type:%v, status:%v, time cost:%ds", id, info.param.TaskType,
				info.status, time.Now().Unix()-info.startTime)
		}
		self.mutex.Unlock()
	}

	runProcess := func() {
		self.watchNewTask()
		self.scanExistingTasks()
		tickCheckNew := time.NewTicker(time.Millisecond * 100).C
		tickPrint := time.NewTicker(time.Second * 60).C
		for {
			select {
			case task := <-self.chTask:
				if !self.stopFetch {
					self.send2Queues(task)
				} else {
					log.Debugf("[%s] fetch stopped, discard", task.param.TaskId)
				}
			case task := <-self.chRemove:
				self.removeTask(task)
			case <-tickCheckNew:
				self.checkNewTasks()
			case <-tickPrint:
				printStatus()
			}
		}
	}

	go runProcess()
	if exitKeepAliveFail {
		self.etcd.StartKeepalive(*self.config.Etcd.KeyPrefix+"/KeepAlive/Worker-"+self.config.InstanceId, 60, 5,
			func() {
				self.logJson(log.LevelCritical, log.Json{"cmd": "keepalive_failed"})
				self.etcd.Exit()
				os.Exit(0)
			})
	}
	log.Infof("Start OK")
}

// 更新任务状态，TaskStatusComplete、TaskStatusError、TaskStatusRestart时child应结束当前任务的执行
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
		self.logJson(log.LevelInfo, log.Json{"cmd": "remove_task", "task_id": param.TaskId,
			"pre_time": preTime, "processing_time": processingTime, "status_str": status,
			"reason": "status error or complete", "status": fmt.Sprintf("%#v", taskInfo.status)})
		self.chRemove <- taskInfo
	}
}

// 停止取新任务，已获取的任务继续执行。升级时部署新的节点并对旧的节点StopFetch，待旧节点任务都处理结束后再删掉
func (self *TaskWorker) StopFetch() {
	self.stopFetch = true
}
