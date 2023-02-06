package master

import (
	"context"
	"encoding/json"
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
	jobKey = common.JOB_SAVE_DIR + job.Name
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
// 删除job
func (jobMgr *JobMgr)DeleteJob(name string)(oldjob *common.Job,err error)  {

	var(
		jobKey string
		deleteresponse *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	//etcd中保存任务的key
	jobKey=common.JOB_SAVE_DIR + name

	// 删除etcd中的任务
	if deleteresponse,err = jobMgr.kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV());err != nil {
		return
	}
	// 将删除的key value返回
	if len(deleteresponse.PrevKvs) != 0 {
		if err = json.Unmarshal(deleteresponse.PrevKvs[0].Value,&oldJobObj); err != nil {
			err = nil
			return
		}
		oldjob = &oldJobObj
		//fmt.Println(oldjob)
	}
	return
}

func (jobMgr *JobMgr)ListJobs()(jobList []*common.Job,err error){
	var (
		dirKey string
		getresp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
		job *common.Job
	)
	dirKey = common.JOB_SAVE_DIR
	if getresp,err = jobMgr.kv.Get(context.TODO(),dirKey,clientv3.WithPrefix());err != nil {
		return
	}
	// 初始化数组空间
	jobList=make([]*common.Job,0)
	// len(joblist0) == 0
	// 遍历所有job,进行反序列化
	for _,kvPair = range getresp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value,job);err != nil {
			err = nil
			continue
		}
		jobList = append(jobList,job)
	}

	return
}

// 杀死任务
func (jobMgr *JobMgr)KillJob(name string)(err error){
	// 更新key=/cron/killer任务名
	var (
		killerKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseid clientv3.LeaseID
	)
	// 通知worker杀死对应任务名
	killerKey = common.JOB_KILLER_DIR + name

	// 让worker监听到一次PUT操作，创建一个租约让其稍后自动过期
	if leaseGrantResp,err = jobMgr.lease.Grant(context.TODO(),1);err != nil {
		return
	}

	// 租约ID
	leaseid = leaseGrantResp.ID

	// 设置killer标记
	if _,err = jobMgr.kv.Put(context.TODO(),killerKey,"",clientv3.WithLease(leaseid));err != nil {
		return
	}


	return
}