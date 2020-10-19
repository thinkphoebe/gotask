package gotask

import (
	"time"

	log "github.com/thinkphoebe/golog"
)

type EtcdConfig struct {
	KeyPrefix   *string        `json:"key_prefix"`
	Endpoints   *[]string      `json:"endpoints"`
	DialTimeout *time.Duration `json:"dial_timeout"`

	// manager only
	LeaseFile      *string `json:"lease_file"`
	SessionTimeout *int    `json:"session_timeout"`
}

type TaskManagerConfig struct {
	Etcd         EtcdConfig `json:"etcd"`
	DeletedKeep  *int64     `json:"delete_keep"`
	FetchMax     *int64     `json:"fetch_max"`
	FetchBatch   *int64     `json:"fetch_batch"`
	FetchTimeout *int64     `json:"fetch_timeout"`

	MaxWaitResource       *int64 `json:"max_wait_resource"`
	WaitResourceThreshold *int64 `json:"wait_resource_threshold"`

	// TaskManager有重要json日志需要打印时调用此回调，用户可在回调中调用log.OutputJson()输出日志
	CbLogJson func(level log.LogLevel, j log.Json) `json:"-"`
}

type ResourceInfo struct {
	Name    string `json:"name"`
	Reserve int    `json:"reserve"`
}

type TaskResource struct {
	Need         int // 预估任务需要的数量，准确或者比实际需要的大一些
	AllocateTime int // 预计从任务启动到该资源分配完毕需要的时间，比实际需要的大一些
	Reserve      int // AllocateTime过后，任务运行过程中需要预留的资源
}

type TaskWorkerConfig struct {
	Etcd EtcdConfig `json:"etcd"`

	// 监控的任务类型，有匹配TaskTypes的任务添加时会尝试获取执行。key: TaskType, value: priority
	// priority值越小，优先级越高
	TaskTypes *map[string]int `json:"task_types"`

	// 资源的类型，key: resource name
	Resources *map[string]*ResourceInfo `json:"resources"`

	// 更新资源占用的最小时间间隔，单位：秒
	ResourceUpdateInterval *int64 `json:"resource_update_interval"`

	// dispatch count超过此阈值时任务会被放入first队列，尝试等待资源处理
	DispatchThreshold *int `json:"dispatch_threshold"`

	// 同一任务最大Wait Resource Worker的数量
	MaxWaitResourceWorkers *int `json:"max_wait_resource_workers"`

	// 队列中超过MaxQueueTime的任务直接删除，不再尝试获取执行
	MaxQueueTime *int64 `json:"max_queue_time"`

	// 任务租约的时间，client需要按小于TaskOwnTime的间隔调用UpdateTaskStatus函数
	// 考虑到网络延迟等因素需要留出一些富余，如在TaskOwnTime过去 2/3时调用，但也不必过于频繁
	TaskOwnTime *int64 `json:"task_own_time"`

	// 该值目的是用来区分client的实例，会写入key Owner的value中，目前没有实际用途
	InstanceId string `json:"-"`

	// 在调用以下CbXxx函数时通过handle参数回传，用于回调函数区分对象实例
	InstanceHandle interface{} `json:"-"`

	CbGetResourceInfo func(handle interface{}, resName string) (total, used int, err error) `json:"-"`
	// 获取任务预估需要占用的资源，仅根据任务参数给出一个安全值，避免非常耗时或block的资源检查
	// key: resType，资源类型，如CPU、GPU、Codec等
	CbGetTaskResources func(handle interface{}, param *TaskParam) (map[string]*TaskResource, error) `json:"-"`

	// 是否允许添加任务
	CbTaskAddCheck func(param *TaskParam) bool `json:"-"`

	// TaskWorker获取任务成功后调用此回调。用户可在此回调中启动执行任务
	// 返回nil表示任务添加成功
	// 返回非nil时将该任务标记为执行失败并Owner，之后manager根据Retry和ErrorCount确定是否重新分发
	CbTaskStart func(handle interface{}, param *TaskParam) error `json:"-"`

	// TaskWorker由于续租失败等原因需要终止任务执行时会调用此回调，用户需要在此回调中停止任务并释放相关的资源
	CbTaskStop func(handle interface{}, param *TaskParam) error `json:"-"`

	// 运行时修改任务参数
	// 如不支持修改可将此函数指针设置为nil
	CbTaskModify func(handle interface{}, param *TaskParam) error `json:"-"`

	// TaskWorker有重要json日志需要打印时调用此回调，用户可在回调中调用log.OutputJson()输出日志
	CbLogJson func(handle interface{}, level log.LogLevel, j log.Json) `json:"-"`
}

type TaskParam struct {
	TaskId        string `json:"task_id,omitempty"`
	TaskType      string `json:"task_type,omitempty"`
	UserId        string `json:"user_id,omitempty"`
	Retry         int    `json:"retry,omitempty"`          // 0：不重试，+n：重试次数，-1：无限重试
	AddTime       int64  `json:"add_time,omitempty"`       // 任务的添加时间，unix second
	DispatchCount int    `json:"dispatch_count,omitempty"` // 已被重新分发的次数
	WaitResource  int    `json:"wait_resource,omitempty"`  // 0：不wait，1：wait
	UserParam     []byte `json:"user_param,omitempty"`
}

type TaskStatus struct {
	Status     string `json:"status,omitempty"`
	StartTime  int64  `json:"start_time,omitempty"`
	UpdateTime int64  `json:"update_time,omitempty"`
	UserParam  []byte `json:"user_param,omitempty"`
}

type WorkerResourceCheck struct {
	TaskId  string `json:"task_id"`
	AddTime int64  `json:"add_time"`
	// 具体某项资源检测是否成功
	ResourceDetails map[string]bool `json:"resource_details"`
	// 整个任务资源检测是否成功，成功需要ResourceDetails均成功且CbTaskAddCheck为nil或返回true
	Succeed bool `json:"succeed"`
}

type FailedResourceInfo struct {
	TaskId           string `json:"task_id"`
	AddTime          int64  `json:"add_time"`
	FailStartTime    int64  `json:"fail_start_time"`
	SucceedTaskCount int64  `json:"succeed_task_count"` // FailStartTime之后成功获取该资源并执行的其它任务数
}

type ErrorInfo struct {
	RetryCount int `json:"retryCount,omitempty"`
}

type DeletedInfo struct {
	DeleteTime int64  `json:"deleteTime,omitempty"`
	TaskParam  []byte `json:"taskParam,omitempty"`
	TaskStatus []byte `json:"taskStatus,omitempty"`
	ErrorInfo  []byte `json:"errorInfo,omitempty"`
}

type TaskInfo struct {
	TaskId     string
	TaskParam  *TaskParam
	TaskStatus *TaskStatus
	RetryCount int
	DeleteTime int64
}

var (
	TaskStatusInit     = "init"
	TaskStatusError    = "error"
	TaskStatusWorking  = "working"
	TaskStatusComplete = "complete"
	TaskStatusRestart  = "restart" // 此状态的任务会立即重新分发，不受retryCount的限制。实时流退出时可使用此状态
)
