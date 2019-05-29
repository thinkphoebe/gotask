package gotask
/*

模块功能：任务分发、排队，支持多用户、多任务类型

/{EtcdKeyPrefix}
├── Users
│   ├── {UsersID 1}
│   ├── {......}
│   └── {UsersID n}
├── TaskTypes				key: live, offline, download, video_frame_extract, audio_split. value: 0, 1
│   ├── {TaskType 1}        value为0时直接分发，value为1时先排队。
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
├── Wait
│   ├── {TaskID 1}
│   ├── {......}
│   └── {TaskID n}
└── Deleted
    ├── {TaskID 1}
    ├── {......}
    └── {TaskID n}

*/
