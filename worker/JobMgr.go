package worker

import (
	"context"
	"fmt"
	"github.com/dengwenjun1986/crontab/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watch clientv3.Watcher
}

var (
	// 单例
	G_jobMgr *JobMgr
)



// 初始化管理器
func InitJobMgr()(err error){

	var(
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		watcher clientv3.Watcher
		)
	config = clientv3.Config{
		Endpoints:[]string{G_config.EtcdEndPoint}, // 集群地址
		DialTimeout:time.Duration(G_config.EtcdDialTimeOut) * time.Millisecond, //链接超时时间
	}

	//建立链接
	if client,err  = clientv3.New(config);err != nil {
		fmt.Println("connect etcd failed!!")
		return
	}

	// 得到kv和lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)
	
	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
		watch:	watcher,
	}

	G_jobMgr = G_jobMgr
	return
}


// 监听任务变化
func(jobMgr *JobMgr) watchJobs(err error){
	var (
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		jobName string
		jobEvent *common.JobEvent

	)
	// 1.get /cron/jobs/目录下的所有任务，并且获知当前集群的revision
	if getResp,err = jobMgr.kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err != nil{
		return
	}
	// 当前有那些任务
	for _,kvpair = range getResp.Kvs {
		// 反序列化json得到job
		if job,err = common.UnpackJob(kvpair.Value);err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)

			fmt.Println(*jobEvent)
			// TODO:  把这个job同步给scheduler（调度协程）
			G_scheduler.PushJobEvent(jobEvent)


		}
	}
	// 2. 从该revision向后监听事件变化
	go func() { // 监听协程
		// 从GET时刻的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		// 监听/cron/jobs/目录的后续变化
		watchChan = jobMgr.watch.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())

		// 处理监听事件
		for watchResp = range watchChan {
			for _,watchEvent = range watchResp.Events{
				switch watchEvent.Type {
					case mvccpb.PUT:// 任务保存事件
					 	if job ,err = common.UnpackJob(watchEvent.Kv.Value);err != nil {
							continue
						}
						// 构建一个更新Event事件
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE,job)

					case mvccpb.DELETE:// 任务被删除
					// Delete /cron/jobs/job10
						jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

						job = &common.Job{Name:jobName}
						// 构建一个删除Event事件
						jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE,job)

						fmt.Println(*jobEvent)

						// TODO：推给scheduler
						G_scheduler.PushJobEvent(jobEvent)
				}
			}

		}
	}()


}