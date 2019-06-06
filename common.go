package gotask

import (
	"errors"
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

	// TaskManager有重要json日志需要打印时调用此回调，用户可在回调中调用log.OutputJson()输出日志
	CbLogJson func(level log.LogLevel, j log.Json)
}

type TaskWorkerConfig struct {
	Etcd EtcdConfig `json:"etcd"`

	// 监控的任务类型，有匹配TaskTypes的任务添加时会尝试获取执行
	TaskTypes *[]string `json:"task_types"`

	// 队列中超过MaxQueueTime的任务直接删除，不再尝试获取执行
	MaxQueueTime *int64 `json:"max_queue_time"`

	// 任务租约的时间，client需要按小于TaskOwnTime的间隔调用UpdateTaskStatus函数
	// 考虑到网络延迟等因素需要留出一些富余，如在TaskOwnTime过去 2/3时调用，但也不必过于频繁
	TaskOwnTime *int64 `json:"task_own_time"`

	// 该值目的是用来区分client的实例，会写入key Owner的value中，目前没有实际用途
	InstanceId string

	// TaskWorker调用此回调确认某个任务是否可以尝试获取并执行
	// 返回nil时表示可以添加任务
	// 返回ErrOutOfResource时表示没有资源，不再继续处理任务列表
	// 返回其它ErrNotSupport时跳过该任务继续处理任务列表
	CbTaskAddCheck func(param *TaskParam) error

	// TaskWorker获取任务成功后调用此回调。用户可在此回调中启动执行任务
	// 返回nil表示任务添加成功
	// 返回非nil时将该任务标记为执行失败并Owner，之后manager根据Retry和ErrorCount确定是否重新分发
	CbTaskStart func(param *TaskParam) error

	// TaskWorker由于续租失败等原因需要终止任务执行时会调用此回调，用户需要在此回调中停止任务并释放相关的资源
	CbTaskStop func(param *TaskParam) error

	// TaskWorker有重要json日志需要打印时调用此回调，用户可在回调中调用log.OutputJson()输出日志
	CbLogJson func(level log.LogLevel, j log.Json)
}

type TaskParam struct {
	TaskId    string `json:"taskId,omitempty"`
	TaskType  string `json:"taskType,omitempty"`
	UserId    string `json:"userId,omitempty"`
	Retry     int    `json:"retry,omitempty"` // 0：不重试，+n：重试次数，-1：无限重试
	AddTime   int64  `json:"add_time,omitempty"` // 任务的添加时间，unix second
	UserParam []byte `json:"userParam,omitempty"`
}

type TaskStatus struct {
	Status     string `json:"status,omitempty"`
	StartTime  int64  `json:"startTime,omitempty"`
	UpdateTime int64  `json:"updateTime,omitempty"`
	UserParam  []byte `json:"userParam,omitempty"`
}

type ErrorInfo struct {
	RetryCount int `json:"retryCount,omitempty"`
}

type DeletedInfo struct {
	DeleteTime int64  `json:"deleteTime,omitempty"`
	TaskParam  []byte `json:"taskParam,omitempty"`
	TaskStatus string `json:"taskStatus,omitempty"`
}

var (
	ErrOutOfResource = errors.New("Out of resource")
	ErrNotSupport    = errors.New("Not Support")
)

var (
	TaskStatusInit     = "init"
	TaskStatusError    = "error"
	TaskStatusWorking  = "working"
	TaskStatusComplete = "complete"
)
