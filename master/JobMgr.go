package master

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dengwenjun1986/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
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
	
	// 赋值单例
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	//G_jobMgr = G_jobMgr
	return
}

// 保存任务
func (jobMgr *JobMgr)SaveJob(job *common.Job)(oldjob *common.Job,err error)  {
	// 把任务保存到/cron/jobs/任务名 -> json
	var (
		jobKey string
		jobValue []byte
		putresp *clientv3.PutResponse
		oldJobObj common.Job
	)

	// etcd的保存key
	jobKey = "/cron/jobs/" + job.Name
	fmt.Println(jobKey)
	//任务信息json
	if jobValue,err = json.Marshal(job); err != nil {
		return
	}
	// 保存到etcd
	if putresp,err = jobMgr.kv.Put(context.TODO(),jobKey,string(jobValue),clientv3.WithPrevKV());err != nil {
		return
	}
	// 如果是 更新，返回old值
	if putresp.PrevKv != nil {
		// 对old值反序列化
		if err = json.Unmarshal(putresp.PrevKv.Value,&oldJobObj); err != nil {
			err = nil
			return
		}
		oldjob = &oldJobObj
	}
	return
}