package common

import (
	"encoding/json"
	"strings"
	"github.com/gorhill/cronexpr"
	"time"
)

// 定时任务
type Job struct {
	Name string `json:"name"` // 任务名
	Command string `json:"command"` // shell命令
	CronExpr string 	`json:"cronExpr"` // 表达式
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job // 要调度的任务信息
	Expr *cronexpr.Expression // 解析好的cronexpr表达式
	NextTime time.Time // 下次调度时间
}

// HTTP接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

type JobEvent struct {
	EventType int // SAVE,DELETE
	Job *Job
}

// 应答方法
func BuildResponse(errno int,msg string,data interface{})(resp []byte,err error)  {
	// 定义一个response
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	// 序列化json
	resp,err = json.Marshal(response)

	return
}

// 反序列化Job
func UnpackJob(value []byte)(ret *Job,err error){
	var (
		job *Job
	)
	job = &Job{}
	if err = json.Unmarshal(value,job);err != nil {
		return
	}
	ret = job
	return
}
// 从ETCD的key中提取任务名
// /cron/jobs/job10 抹掉/cron/jobs/
func ExtractJobName(jobKey string)(string){
	return strings.TrimPrefix(jobKey,JOB_SAVE_DIR)
}

// 任务变化事件有2种：1）更新任务 2）删除任务
func BuildJobEvent(eventType int,job *Job)(jobEvent *JobEvent){
	return &JobEvent{
		EventType:eventType,
		Job:job,
	}
}

// 构造执行计划
func BuildJobSchedulePlan(job *Job)(jobSchedulePlan *JobSchedulePlan,err error){
	var (
		expr *cronexpr.Expression
	)
	// 解析JOB的cron表达式
	if expr,err = cronexpr.Parse(job.CronExpr);err != nil {
		return
	}
	// 生成任务调度计划对象
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}