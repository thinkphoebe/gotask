package gotask
/*

# 模块功能：任务分发、排队，支持多用户、多任务类型

/{EtcdKeyPrefix}
├── Users
│   ├── {UsersID 1}          Manager写入，记录创建任务的用户信息
│   ├── {......}
│   └── {UsersID n}
├── TaskTypes                Manager写入，记录任务类型，key: live, offline, download
│   ├── {TaskType 1}
│   ├── {......}
│   └── {TaskType n}
├── ResourceTypes            Manager写入，记录资源类型，来自Worker执行任务时的Resource检测结果，如GPU、GPU_MEM、CPU
│   ├── {ResourceType 1}
│   ├── {......}
│   └── {ResourceType n}
├── TaskParam                Manager写入，记录任务的参数信息
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── Queue                    Manager写入，用于任务排队
│   ├── {TaskType 1}
│   │   ├── {User 1}
│   │   │   ├── {TaskID 1}
│   │   │   ├── {......}
│   │   │   └── {TaskID n}
│   │   ├── {......}
│   │   └── {User n}
│   ├── {......}
│   └── {TaskType n}
├── Processing               任务排队结束后，将Queue中的对应节点移动到Processing下，并在Fetch下创建有timeout的节点
│   ├── {TaskID 1}           Master启动时遍历Processing并重新分发无Owner的任务，防止中断期间有Fetch节点超时没有被重新下发
│   ├── {......}
│   └── {TaskID n}
├── Fetch                    Worker监控支持的TaskType对应的Fetch Prefix，有新节点创建时尝试抢占、执行任务
│   ├── {TaskType 1}
│   │   ├── {TaskID 1}
│   │   ├── {......}
│   │   └── {TaskID n}
│   ├── {......}
│   └── {TaskType n}
├── Owner                    Worker写入，只有一个写入成功，用于Worker抢占任务，Manager监控执行任务的Worker是否挂掉
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── Status                   Worker写入，Manager端仅读取
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── ErrorInfo                Manager写入，记录任务重试次数等信息
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── WaitRecover              Manager写入，用做Timer，控制任务恢复的延迟时间
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── WaitResource             Worker写入，用于控制一个任务上WaitResource的Worker数量
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── WorkerResourceCheck      Worker写入，记录任务资源检测的结果，可能不是每次检测都写，Manager Watch读取
│   ├── {Random 1}
│   ├── {......}
│   └── {Random n}
├── FailedResource           Manager写入，记录资源检测失败的任务及失败信息，定期遍历将适当的任务标记为需要WaitResource
│   ├── {ResourceType 1}
│   │   ├── {TaskID 1}
│   │   ├── {......}
│   │   └── {TaskID n}
│   ├── {......}
│   └── {ResourceType n}
└── Deleted                  Manager写入，用于记录近期删除的任务信息供Query、List等查询接口使用，有个goroutine会定期清理
    ├── {TaskID 1}
    ├── {......}
    └── {TaskID n}


# 三种资源管理模式
1. 完全由TaskWorker管理
	* TaskWorkerConfig设置Resources、ResourceUpdateInterval、CbGetResourceInfo、CbGetTaskResources为有效值
	* CbTaskAddCheck设置为nil或固定返回true
2. 完全由Child管理
	* TaskWorkerConfig设置CbGetResourceInfo、CbGetTaskResources为nil
	* CbTaskAddCheck设置为非nil，并根据是否允许添加任务返回true或false
3. TaskWorker和Child共同管理
	* TaskWorkerConfig设置Resources、ResourceUpdateInterval、CbGetResourceInfo、CbGetTaskResources为有效值
	* CbTaskAddCheck设置为非nil，并根据是否允许添加任务返回true或false
	* TaskWorker根据CbGetResourceInfo、CbGetTaskResources等信息检查满足资源需求，且CbTaskAddCheck返回true时尝试获取任务

*/
