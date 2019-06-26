package gotask

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/thinkphoebe/goetcd"
	log "github.com/thinkphoebe/golog"
)

type _FetchInfo struct {
	add      int64
	user     string
	taskType string
}

type TaskManager struct {
	config          TaskManagerConfig
	etcd            goetcd.Etcd
	electionSession *concurrency.Session

	//保存各个userId
	usersMap map[string]int64

	//保存各个taskType
	taskTypesMap map[string]int64
	//为避免多线程读写将修改值发送到此chan统一修改
	chTaskTypesUpdate chan string

	//从该chan读取到数据时更新fetch，值为GFetchCount需要更新的差值
	chFetchUpdate chan _FetchInfo
	//记录fetch节点的数量
	fetchCount map[string]int64
}

func getKeyEnd(key string) string {
	secs := strings.Split(key, "/")
	if len(secs) == 0 {
		log.Errorf("FAILED! [%s]", key)
		return ""
	}
	return secs[len(secs)-1]
}

func genTaskId() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	h := md5.New()
	h.Write([]byte(base64.URLEncoding.EncodeToString(b)))
	return hex.EncodeToString(h.Sum(nil))
}

func (self *TaskManager) readEtcdJson(taskId string, key string, value_out *[]byte, object_out interface{}) error {
	vals, err := self.etcd.Get(key, false)
	if err != nil || len(vals) == 0 {
		log.Infof("[%s] etcd.Get [%s] err:%v, len vals [%d]", taskId, key, err, len(vals))
		return errors.New(fmt.Sprintf("etcd Get [%v], len vals [%d]", err, len(vals)))
	}
	if value_out != nil {
		*value_out = vals[0]
	}
	if object_out != nil {
		if err := json.Unmarshal(vals[0], &object_out); err != nil {
			log.Infof("[%s] json.Unmarshal got err:%v, key:%s, readed value [%s]", taskId, err, key, vals[0])
			return err
		}
	}
	return nil
}

func (self *TaskManager) itemKey(taskId, item string) string {
	return *self.config.Etcd.KeyPrefix + "/" + item + "/" + taskId
}

func (self *TaskManager) onKeyOwnerDelete(key string, val []byte) bool {
	taskId := getKeyEnd(key)

	var taskParam TaskParam
	if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), nil, &taskParam) != nil {
		log.Debugf("[%s] read TaskParam FAILED, task may be deleted", taskId)
		return true
	}

	var retryCount = 0
	var errInfo ErrorInfo
	if self.readEtcdJson(taskId, self.itemKey(taskId, "ErrorInfo"), nil, &errInfo) == nil {
		retryCount = errInfo.RetryCount
		log.Debugf("[%s] read RetryCount [%d]", taskId, retryCount)
	}

	var taskStatus TaskStatus
	if self.readEtcdJson(taskId, self.itemKey(taskId, "Status"), nil, &taskStatus) != nil ||
		taskStatus.Status == "complete" {
		//任务正常结束
		log.Infof("[%s] remove complete task, status:%v, retryCount:%d", taskId, taskStatus, retryCount)
		self.removeTask(taskId, "delete_complete")
	} else {
		if taskParam.Retry < 0 || retryCount <= taskParam.Retry {
			waitTime := int64(math.Pow(float64(retryCount)*3, 1.6))
			if waitTime > 100 {
				waitTime = 100
			}
			if waitTime <= 0 {
				waitTime = 1
			}
			logId := "recover_error"
			if taskStatus.Status == "working" {
				logId = "recover_timeout"
			}

			log.Infof("[%s] wait %ds recover task, status:%v, retryCount:%d", taskId, waitTime, taskStatus, retryCount)
			key := self.itemKey(taskId, "Wait")
			val := logId
			if err := self.etcd.Put(key, val, waitTime); err != nil {
				self.config.CbLogJson(log.LevelCritical, log.Json{"cmd": "task_lost", "task_id": taskId, "key": key,
					"value": logId, "ttl": waitTime})
			}
		} else {
			//删除出错任务
			log.Infof("[%s] remove error task, status:%v, retryCount:%d", taskId, taskStatus, int(retryCount))
			self.removeTask(taskId, "delete_error")
		}
	}

	return true
}

func (self *TaskManager) onKeyFetchDelete(key string, val []byte) bool {
	taskId := getKeyEnd(key)
	var taskParam TaskParam
	if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), nil, &taskParam) != nil {
		log.Infof("[%s] read TaskParam FAILED, task may be deleted, skip recover", taskId)
		return true
	}
	if self.readEtcdJson(taskId, self.itemKey(taskId, "Owner"), nil, nil) != nil {
		log.Infof("[%s] no owner, recover task", taskId)
		self.config.CbLogJson(log.LevelWarn, log.Json{"cmd": "fetch_timeout", "task_id": taskId, "task_type": taskParam.TaskType})
		self.recoverTask(taskId, "recover_fetch", false)
	}
	return true
}

func (self *TaskManager) onKeyWaitDelete(key string, val []byte) bool {
	taskId := getKeyEnd(key)
	if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), nil, nil) != nil {
		log.Infof("[%s] read TaskParam FAILED, task may be deleted, skip recover", taskId)
		return true
	}
	log.Infof("[%s] wait complete, recover task", taskId)
	self.recoverTask(taskId, string(val), true)
	return true
}

func (self *TaskManager) initUsers() {
	onUser := func(key string, val []byte) bool {
		user := getKeyEnd(key)
		self.usersMap[user] = time.Now().Unix()
		log.Infof("find user [%s]", user)
		return true
	}
	self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/Users/", onUser, -1, nil)

	onTaskType := func(key string, val []byte) bool {
		taskType := getKeyEnd(key)
		self.chTaskTypesUpdate <- taskType
		log.Infof("find taskType [%s]", taskType)
		return true
	}
	self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/TaskTypes/", onTaskType, -1, nil)
}

func (self *TaskManager) watchQueue() {
	on_put := func(key string, val []byte) bool {
		secs := strings.Split(key, "/")
		// /Queue/{TaskType}/{UserId}/{TaskId}
		if len(secs) < 4 {
			return true
		}
		taskId := secs[len(secs)-1]
		user := secs[len(secs)-2]
		taskType := secs[len(secs)-3]
		self.chTaskTypesUpdate <- taskType
		self.chFetchUpdate <- _FetchInfo{add: 0, user: user, taskType: taskType}
		log.Debugf("Queue [%s] new task [%s] for user [%s]", taskType, taskId, user)
		return true
	}
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Queue/", "PUT", true, on_put, nil)
}

func (self *TaskManager) watchFetch() {
	for taskType := range self.taskTypesMap {
		count, err := self.etcd.Count(*self.config.Etcd.KeyPrefix + "/Fetch/" + taskType)
		if err != nil {
			log.Infof("etcd.Count [Fetch/%s] got error [%s]", taskType, err.Error())
		} else {
			self.fetchCount[taskType] = count
			log.Debugf("init fetchCount [%s] to [%d]", taskType, count)
		}
	}

	onPut := func(key string, val []byte) bool {
		// /Fetch/{TaskType}/{taskId}
		secs := strings.Split(key, "/")
		if len(secs) < 3 {
			return true
		}
		self.chFetchUpdate <- _FetchInfo{add: 1, taskType: secs[len(secs)-2]}
		log.Debugf("Fetch have key put [%s]", key)
		return true
	}
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Fetch/", "PUT", true, onPut, nil)

	onDel := func(key string, val []byte) bool {
		secs := strings.Split(key, "/")
		if len(secs) < 3 {
			return true
		}
		self.chFetchUpdate <- _FetchInfo{add: -1, taskType: secs[len(secs)-2]}
		log.Debugf("Fetch have key delete [%s]", key)
		return true
	}
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Fetch/", "DELETE", true, onDel, nil)
}

func (self *TaskManager) updateFetch() {
	var queues = make(map[string][]string)

	// 将任务从Queue移动到Processing中，并添加Fetch
	moveTask := func(key string) {
		taskId := getKeyEnd(key)
		var taskParam TaskParam
		var paramBytes []byte
		if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), &paramBytes, &taskParam) != nil {
			log.Infof("[%s] read TaskParam FAILED! task may be deleted", key)
			self.etcd.Del(key, false)
			return
		}

		cmpParam := clientv3.Compare(clientv3.CreateRevision(self.itemKey(taskId, "TaskParam")), "!=", 0)
		opProc, _ := self.etcd.OpPut(self.itemKey(taskId, "Processing"), string(paramBytes), 0)
		opQueue := self.etcd.OpDel(self.itemKey(taskId, "Queue/"+taskParam.TaskType+"/"+taskParam.UserId), false)
		opFetch, err := self.etcd.OpPut(self.itemKey(taskId, "Fetch/"+taskParam.TaskType), string(paramBytes), *self.config.FetchTimeout)
		if err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_grant", "task_id": taskId,
				"ttl": self.config.FetchTimeout, "error": "moveTask self.etcd.OpPut got err:" + err.Error()})
			return
		}
		ifs := []clientv3.Op{*opProc, *opQueue, *opFetch}
		resp, err := self.etcd.Txn([]clientv3.Cmp{cmpParam}, ifs, nil)
		if err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_txn", "task_id": taskId,
				"error": "moveTask self.etcd.Txn got err:" + err.Error()})
			return
		}
		if !resp.Succeeded {
			log.Infof("moveTask [%s] resp not Succeed!", key)
			return
		}

		log.Debugf("move task succeed [%s]", key)
	}

	// 从各个user的的队列里均匀地取FetchBatch个任务，放入一个任务队列
	feedQueue := func(taskType string) int {
		total := 0
		for userId := range self.usersMap {
			count := 0
			addQueue := func(key string, val []byte) bool {
				queues[taskType] = append(queues[taskType], key)
				count++
				total++
				log.Debugf("feed [%s] to queue", key)
				return true
			}
			self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/Queue/"+taskType+"/"+userId, addQueue, *self.config.FetchBatch, nil)
		}
		return total
	}

	// 检查/Fetch/{TaskType}下的节点数，小于FetchMax时从队列中填充些任务进来
	// 返回值的含义是该taskType是否可删除
	feedFetch := func(taskType string, full bool) bool {
		// 大部分情况下使用fetchCount变量记录的值，为避免和fetchCount和etcd的实际记录不一致，定期读取etcd.Count并更新fetchCount
		if full {
			count, err := self.etcd.Count(*self.config.Etcd.KeyPrefix + "/Fetch/" + taskType)
			if err != nil {
				log.Infof("etcd.Count [Fetch/%s] got error [%s]", taskType, err.Error())
			} else {
				self.fetchCount[taskType] = count
				log.Debugf("update fetchCount to [%s][%d]", taskType, count)
			}
		}

		feedMax := int(*self.config.FetchMax - self.fetchCount[taskType])
		for i := 0; i < feedMax; i++ {
			log.Debugf("taskType:%s, queues:%d, feedMax:%d, i:%d", taskType, len(queues[taskType]), feedMax, i)
			if len(queues[taskType]) == 0 {
				feedCount := feedQueue(taskType)
				log.Debugf("feedCount:%d", feedCount)
				if feedCount == 0 {
					return true
				}
			}
			moveTask(queues[taskType][0])
			queues[taskType] = queues[taskType][1:]
		}

		return false
	}

	tickChan := time.NewTicker(time.Second * 60).C
	lastUserRemove := time.Now().Unix()
	for {
		select {
		case fetchUPdate := <-self.chFetchUpdate:
			if fetchUPdate.user != "" {
				if _, ok := self.usersMap[fetchUPdate.user]; !ok {
					log.Infof("add user [%s]", fetchUPdate.user)
				}
				self.usersMap[fetchUPdate.user] = time.Now().Unix()
			}

			// 根据 /Fetch/{TaskType}/下节点的变化情况更新fetchCount变量的值
			self.fetchCount[fetchUPdate.taskType] += fetchUPdate.add
			if len(self.chFetchUpdate) <= 1 && self.fetchCount[fetchUPdate.taskType] < *self.config.FetchMax {
				log.Debugf("chFetchUpdate [%d], fetchCount [%s][%d], feed fetch", len(self.chFetchUpdate),
					fetchUPdate.taskType, self.fetchCount[fetchUPdate.taskType])
				feedFetch(fetchUPdate.taskType, false)
			} else {
				log.Debugf("chFetchUpdate [%d], fetchCount [%s][%d], skip feed", len(self.chFetchUpdate),
					fetchUPdate.taskType, self.fetchCount[fetchUPdate.taskType])
			}
		case <-tickChan:
			log.Debugf("feed fetch for tick")

			// 定期feedFetch，并清理没有任务的taskType
			remove := make([]string, 0)
			for taskType, update := range self.taskTypesMap {
				if feedFetch(taskType, true) && time.Now().Unix()-update > 300 && len(queues[taskType]) == 0 {
					remove = append(remove, taskType)
				}
			}
			for _, taskType := range remove {
				log.Infof("remove taskType [%s]", taskType)
				self.etcd.Del(*self.config.Etcd.KeyPrefix+"/TaskTypes/"+taskType, false)
				delete(self.taskTypesMap, taskType)
				delete(queues, taskType)
			}

			// 定期清理没有任务的user
			if time.Now().Unix()-lastUserRemove > 600 {
				for userId, update := range self.usersMap {
					var total int64 = 0
					haveError := false
					for taskType := range self.taskTypesMap {
						count, err := self.etcd.Count(*self.config.Etcd.KeyPrefix + "/Queue/" + taskType + "/" + userId)
						self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "user_queue", "user_id": userId,
							"queue_size": count, "taskType": taskType, "update": time.Now().Unix() - update})
						if err != nil {
							haveError = true
						}
						total += count
					}
					if !haveError && time.Now().Unix()-update > 300 && total == 0 {
						log.Debugf("remove user [%s]", userId)
						self.etcd.Del(*self.config.Etcd.KeyPrefix+"/Users/"+userId, false)
						delete(self.usersMap, userId)
					}
				}
				lastUserRemove = time.Now().Unix()
			}
		case taskTypesUpdate := <-self.chTaskTypesUpdate:
			// 有新的taskType时创建对应的变量，已有的taskType更新Update时间
			if _, ok := self.taskTypesMap[taskTypesUpdate]; !ok {
				log.Infof("add taskType [%s]", taskTypesUpdate)
			}
			self.taskTypesMap[taskTypesUpdate] = time.Now().Unix()
			if _, ok := queues[taskTypesUpdate]; !ok {
				queues[taskTypesUpdate] = make([]string, 0)
			}
		}
	}
}

func (self *TaskManager) recoverTask(taskId string, logId string, haveError bool) bool {
	var taskParam TaskParam
	var paramBytes []byte
	if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), &paramBytes, &taskParam) != nil {
		log.Errorf("[%s] read TaskParam FAILED!", taskId)
		return false
	}

	var errInfo ErrorInfo
	if haveError {
		if self.readEtcdJson(taskId, self.itemKey(taskId, "ErrorInfo"), nil, &errInfo) == nil {
			log.Debugf("[%s] read ErrorInfo [%v]", taskId, errInfo)
		}
		errInfo.RetryCount += 1

		errorBytes, _ := json.Marshal(errInfo)
		log.Infof("[%s] update ErrorInfo to:%v", taskId, string(errorBytes))
		key := self.itemKey(taskId, "ErrorInfo")
		if err := self.etcd.Put(key, string(errorBytes), 0); err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_write", "task_id": taskId, "key": key,
				"value": string(errorBytes), "ttl": 0, "error": err.Error()})
		}
	}

	log.Errorf("[%s] recover task", taskId)
	key := self.itemKey(taskId, "Fetch/"+taskParam.TaskType)
	if err := self.etcd.Put(key, string(paramBytes), *self.config.FetchTimeout); err != nil {
		self.config.CbLogJson(log.LevelCritical, log.Json{"cmd": "task_lost", "task_id": taskId, "key": key,
			"value": string(paramBytes), "ttl": 0})
	}

	var statesBytes []byte
	self.readEtcdJson(taskId, self.itemKey(taskId, "Status"), &statesBytes, nil)
	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": logId, "task_id": taskId, "user_id": taskParam.UserId,
		"task_type": taskParam.TaskType, "task_status": string(statesBytes), "error_count": errInfo.RetryCount})
	return true
}

func (self *TaskManager) removeTask(taskId string, logId string) error {
	var taskParam TaskParam
	var paramBytes []byte
	if self.readEtcdJson(taskId, self.itemKey(taskId, "TaskParam"), &paramBytes, &taskParam) != nil {
		log.Errorf("[%s] read TaskParam FAILED, task may be deleted", taskId)
		return errors.New("task not exist")
	}

	var taskStatus TaskStatus
	var statusBytes []byte
	if self.readEtcdJson(taskId, self.itemKey(taskId, "Status"), &statusBytes, &taskStatus) != nil {
		taskStatus.Status = "waiting"
		statusBytes, _ = json.Marshal(&taskStatus)
	}

	var delInfo DeletedInfo
	delInfo.TaskParam = paramBytes
	delInfo.TaskStatus = string(statusBytes)
	delInfo.DeleteTime = time.Now().Unix()
	delBytes, _ := json.Marshal(delInfo)

	opInfo := self.etcd.OpDel(self.itemKey(taskId, "TaskParam"), false)
	opQueue := self.etcd.OpDel(self.itemKey(taskId, "Queue/"+taskParam.TaskType+"/"+taskParam.UserId), false)
	opProc := self.etcd.OpDel(self.itemKey(taskId, "Processing"), false)
	opFetch := self.etcd.OpDel(self.itemKey(taskId, "Fetch/"+taskParam.TaskType), false)
	opOwner := self.etcd.OpDel(self.itemKey(taskId, "Owner"), false)
	opStatus := self.etcd.OpDel(self.itemKey(taskId, "Status"), false)
	opErrorInfo := self.etcd.OpDel(self.itemKey(taskId, "ErrorInfo"), false)
	opWait := self.etcd.OpDel(self.itemKey(taskId, "Wait"), false)
	opDel, _ := self.etcd.OpPut(self.itemKey(taskId, "Deleted"), string(delBytes), 0)

	ifs := []clientv3.Op{*opDel, *opInfo, *opQueue, *opProc, *opStatus, *opFetch, *opOwner, *opWait, *opErrorInfo}
	resp, err := self.etcd.Txn(nil, ifs, nil)
	if err != nil {
		self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_txn", "task_id": taskId,
			"error": "removeTask etcd.Txn got err:" + err.Error()})
		return errors.New("etcd.Txn got err:" + err.Error())
	}
	if !resp.Succeeded {
		self.config.CbLogJson(log.LevelCritical, log.Json{"cmd": "etcd_txn", "task_id": taskId,
			"error": "removeTask etcd.Txn resp not Succeeded, should not go here"})
		return errors.New("etcd.Txn resp not Succeeded")
	}

	// 出错删除时记录任务参数以便重做任务
	param := "ommit"
	if logId == "delete_error" {
		param = string(paramBytes)
	}
	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": logId, "task_id": taskId, "user_id": taskParam.UserId,
		"task_type": taskParam.TaskType, "task_status": taskStatus, "task_param": param})
	return nil
}

func (self *TaskManager) recoverCallback(key string, val []byte) bool {
	taskId := getKeyEnd(key)
	if self.readEtcdJson(taskId, self.itemKey(taskId, "Owner"), nil, nil) != nil {
		// 由于处理逻辑和Owner被删除相同，这里直接调用onKeyOwnerDelete
		self.onKeyOwnerDelete(*self.config.Etcd.KeyPrefix+"/Owner/"+taskId, nil)
	}
	return true
}

func (self *TaskManager) deletedCleanCallback(key string, val []byte) bool {
	var delInfo DeletedInfo
	err := json.Unmarshal(val, &delInfo)
	if err != nil {
		log.Errorf("parse delInfo FAILED! delete key. key [%s], err [%s]", key, err.Error())
		if _, err := self.etcd.Del(key, false); err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_del", "key": key, "error": err.Error()})
		}
		return true
	}
	if time.Now().Unix()-delInfo.DeleteTime < *self.config.DeletedKeep {
		log.Infof("key [%s] deleteTime [%v], now [%v], stop clean", key, delInfo.DeleteTime, time.Now().Unix())
		return false
	}

	log.Infof("key timeout! delete key. [%s] [%v]", key, delInfo.DeleteTime)
	if _, err := self.etcd.Del(key, false); err != nil {
		self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_del", "key": key, "error": err.Error()})
	}
	return true
}

func (self *TaskManager) statusCleanCallback(key string, val []byte) bool {
	var taskStatus TaskStatus
	err := json.Unmarshal(val, &taskStatus)
	if err != nil {
		log.Errorf("parse taskStatus FAILED! delete key. key [%s], err [%s]", key, err.Error())
		if _, err := self.etcd.Del(key, false); err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_del", "key": key, "error": err.Error()})
		}
		return true
	}
	if time.Now().Unix()-taskStatus.UpdateTime > 3600 {
		log.Infof("found timeout status key! delete key. [%s] [%v]", key, taskStatus.UpdateTime)
		if _, err := self.etcd.Del(key, false); err != nil {
			self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_del", "key": key, "error": err.Error()})
		}
	}
	return true
}

func (self *TaskManager) Init(config *TaskManagerConfig) error {
	self.config = *config
	err := self.etcd.Init(*config.Etcd.Endpoints, *config.Etcd.DialTimeout)
	if err != nil {
		log.Criticalf("etcd.Init got err [%s], Endpoints [%#v], DialTimeout [%d]",
			err.Error(), config.Etcd.Endpoints, config.Etcd.DialTimeout)
		return err
	}
	self.chTaskTypesUpdate = make(chan string, 10)
	self.chFetchUpdate = make(chan _FetchInfo, 100)
	self.usersMap = make(map[string]int64)
	self.taskTypesMap = make(map[string]int64)
	self.fetchCount = make(map[string]int64)
	log.Infof("Init OK")
	return nil
}

// ATTENTION 本函数会调用os.Exit()使整个程序退出
func (self *TaskManager) Exit() error {
	log.Infof("Exit...")
	self.electionSession.Close() //使slave立即election succeed提供服务
	self.etcd.Exit()
	os.Exit(0)
	return nil
}

func (self *TaskManager) Start() {
	self.electionSession = goetcd.DoElection(self.etcd.Client, *self.config.Etcd.SessionTimeout,
		*self.config.Etcd.KeyPrefix+"/Election", *self.config.Etcd.LeaseFile)

	go func(c <-chan struct{}) {
		<-c
		log.Errorf("lost master session, exit")
		self.Exit()
	}(self.electionSession.Done())

	self.usersMap = make(map[string]int64)
	self.chFetchUpdate = make(chan _FetchInfo, 100)
	self.fetchCount = make(map[string]int64)
	self.watchQueue()
	self.initUsers()
	self.watchFetch()
	go self.updateFetch()

	log.Warnf("start to handle as master")
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Fetch/", "DELETE", true, self.onKeyFetchDelete, nil)
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Owner/", "DELETE", true, self.onKeyOwnerDelete, nil)
	go self.etcd.WatchCallback(*self.config.Etcd.KeyPrefix+"/Wait/", "DELETE", true, self.onKeyWaitDelete, nil)

	//为了防止master切换期间有fetch timer超时没有watch导致超时没有取的任务一直没有重新下发，这里扫描一遍
	log.Warnf("start recover...")
	self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/Processing/", self.recoverCallback, -1, nil)
	log.Warnf("recover completed")

	//定期清理${task_root}/deleted中过期的key
	for {
		log.Warnf("start deleted clean...")
		opts := []clientv3.OpOption{}
		opts = append(opts, clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend))
		self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/Deleted/", self.deletedCleanCallback, -1, opts)
		log.Warnf("start status clean...")
		self.etcd.WalkCallback(*self.config.Etcd.KeyPrefix+"/Status/", self.statusCleanCallback, -1, opts)
		log.Warnf("clean completed")
		time.Sleep(time.Second * 3600)
	}
}

func (self *TaskManager) Add(userId, taskType string, userParam []byte, retryCount int) (taskId string, err error) {
	var taskParam TaskParam
	taskId = genTaskId()
	taskParam.TaskId = taskId
	taskParam.TaskType = taskType
	taskParam.UserId = userId
	taskParam.Retry = retryCount
	taskParam.AddTime = time.Now().Unix()
	taskParam.UserParam = userParam
	paramBytes, _ := json.Marshal(taskParam)

	opInfo, _ := self.etcd.OpPut(self.itemKey(taskId, "TaskParam"), string(paramBytes), 0)
	ifs := []clientv3.Op{*opInfo}
	opUser, _ := self.etcd.OpPut(*self.config.Etcd.KeyPrefix+"/Users/"+userId, "", 0)
	opTaskType, _ := self.etcd.OpPut(*self.config.Etcd.KeyPrefix+"/TaskTypes/"+taskType, "", 0)
	opQueue, _ := self.etcd.OpPut(self.itemKey(taskId, "Queue/"+taskType+"/"+userId), "", 0)
	ifs = append(ifs, *opUser, *opTaskType, *opQueue)

	resp, err := self.etcd.Txn(nil, ifs, nil)
	if err != nil {
		self.config.CbLogJson(log.LevelError, log.Json{"cmd": "etcd_txn", "task_id": taskId,
			"error": "etcd.Txn got err:" + err.Error()})
		return "", errors.New("etcd.Txn got err:" + err.Error())
	}

	if !resp.Succeeded {
		self.config.CbLogJson(log.LevelCritical, log.Json{"cmd": "etcd_txn", "task_id": taskId,
			"error": "etcd.Txn resp not Succeeded, should not go here"})
		return "", errors.New("etcd.Txn resp not Succeeded")
	}

	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "task_add", "task_id": taskId, "user_id": userId,
		"task_type": taskType, "user_param": userParam, "retry": retryCount})
	return taskId, nil
}

func (self *TaskManager) Remove(taskId string) error {
	err := self.removeTask(taskId, "delete_user")
	self.config.CbLogJson(log.LevelInfo, log.Json{"cmd": "task_remove", "task_id": taskId, "err": fmt.Sprintf("%v", err)})
	return err
}

func (self *TaskManager) Query(taskId string) error {
	//TODO
	return nil
}

func (self *TaskManager) List() error {
	//TODO
	return nil
}
