package gotask

import (
	"errors"
	"time"

	log "github.com/thinkphoebe/golog"
)

type EtcdConfig struct {
	KeyPrefix   string
	Endpoints   []string
	DialTimeout time.Duration

	// manager only
	LeaseFile      string
	SessionTimeout int
}

type ManagerConfig struct {
	Etcd         EtcdConfig

	DeletedKeep  int64

	FetchMax     int64
	FetchBatch   int64
	FetchTimeout int64

	// TaskManager有重要json日志需要打印时调用此回调，用户可在回调中调用log.OutputJson()输出日志
	CbLogJson func(level log.LogLevel, j log.Json)
}

type WorkerConfig struct {
	Etcd EtcdConfig

	// 监控的任务类型
	TaskTypes []string

	// 队列中超过MaxQueueTime的任务直接删除，不在尝试获取执行
	MaxQueueTime int64

	// 任务租约的时间，client需要按小于TaskOwnTime的间隔调用UpdateTaskStatus函数
	// 考虑到网络延迟等因素需要留出一些富余，如在TaskOwnTime过去 2/3时调用，但也不必过于频繁
	TaskOwnTime int64

	// 该值目的是用来区分client的实例，会写入key Owner的value中，目前没有实际用途
	GInstanceId string

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
	UserParam string `json:"userParam,omitempty"`
}

type TaskStatus struct {
	Status     string `json:"status,omitempty"`
	StartTime  int64  `json:"startTime,omitempty"`
	UpdateTime int64  `json:"updateTime,omitempty"`
	UserParam  string `json:"userParam,omitempty"`
}

type ErrorInfo struct {
	RetryCount int `json:"retryCount,omitempty"`
}

type DeletedInfo struct {
	DeleteTime int64  `json:"deleteTime,omitempty"`
	TaskParam  string `json:"taskParam,omitempty"`
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
