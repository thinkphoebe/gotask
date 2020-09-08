package gotask
/*

# 模块功能：任务分发、排队，支持多用户、多任务类型

/{EtcdKeyPrefix}
├── Users
│   ├── {UsersID 1}
│   ├── {......}
│   └── {UsersID n}
├── TaskTypes				key: live, offline, download, video_frame_extract, audio_split
│   ├── {TaskType 1}
│   ├── {......}
│   └── {TaskType n}
├── TaskParam
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── Queue
│   ├── {TaskType 1}
│   │   ├── {User 1}
│   │   │   ├── {TaskID 1}
│   │   │   ├── {......}
│   │   │   └── {TaskID n}
│   │   ├── {......}
│   │   └── {User n}
│   ├── {......}
│   └── {TaskType n}
├── Processing              任务排队结束后，将Queue中的对应节点移动到Processing下，并在Fetch下创建有timeout的节点
│   ├── {TaskID 1}          Master启动时遍历Processing并重新分发无Owner的任务，防止中断期间有Fetch节点超时没有被重新下发
│   ├── {......}
│   └── {TaskID n}
├── Fetch
│   ├── {TaskType 1}
│   │   ├── {TaskID 1}
│   │   ├── {......}
│   │   └── {TaskID n}
│   ├── {......}
│   └── {TaskType n}
├── Owner
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── Status                  Worker端写入，Manager端仅读取
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── ErrorInfo               Manager端写入，记录任务重试次数等信息
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── WaitRecover
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
├── WaitResource
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
└── Deleted
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
