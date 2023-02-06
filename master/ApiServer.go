package master

import (
	"encoding/json"
	"fmt"
	"github.com/dengwenjun1986/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var(
	// 单例对象
	G_apiServer *ApiServer
)

// 保存任务接口
	// 获取任务
		// POST job = {"name":"job1","command":"echo hello","cronExpr":"* * * * * * *"}
func handleJobSave(resp http.ResponseWriter,req *http.Request){
	var (
		err error
		postJob string
		job common.Job
		oldjob *common.Job
		bytes []byte
	)
			// 1.	解析POST表单
if err = req.ParseForm();err != nil {
	goto ERR
}
		// 2. 取表单中的job字段
postJob = req.PostForm.Get("job")
		// 3.反序列化job
if err = json.Unmarshal([]byte(postJob),&job);err != nil {
	goto ERR
}
		// 4.保存任务到ETCD中
if oldjob,err = G_jobMgr.SaveJob(&job);err != nil {
	goto ERR
}
		//5.正常应答 ({"errno":0,"msg":"",data:"{...}" })
		if bytes,err = common.BuildResponse(0,"success",oldjob);err == nil {
			fmt.Println(string(bytes))
			_, _ = resp.Write(bytes)
		}
	return
ERR:
		//6.异常应答
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err != nil {
		_, _ = resp.Write(bytes)
	}
}

// job删除
// POST /job/delete name=job1
func handleJobDelete(resp http.ResponseWriter,req *http.Request){
	var (
		err error
		name string
		oldjob *common.Job
		bytes []byte
	)
	//POST a=1&b=2&c=3
	if err = req.ParseForm();err != nil {
		goto ERR
	}
	// 删除的任务名
	name = req.PostForm.Get("name")
	//fmt.Println(name)
	//删除任务
	if oldjob,err = G_jobMgr.DeleteJob(name);err != nil {
		//fmt.Println(oldjob)
		goto ERR
	}

	// 正常应答
	if bytes,err = common.BuildResponse(0,"success",oldjob);err == nil {
		//fmt.Printf("删除后返回旧值：%s\n",string(bytes))
		_, _ = resp.Write(bytes)
	}
	return
	ERR:
		// 异常应答
		if bytes,err = common.BuildResponse(-1,err.Error(),nil);err == nil {
			_, _ = resp.Write(bytes)
		}
}
// job list
func handleJobList(resp http.ResponseWriter,req *http.Request){
	var (
		joblist []*common.Job
		err error
		bytes []byte
	)
	if joblist,err = G_jobMgr.ListJobs();err != nil {
		goto ERR
	}
	// 正常应答
	if bytes,err = common.BuildResponse(0,"success",joblist);err == nil {
		//fmt.Printf("删除后返回旧值：%s\n",string(bytes))
		_, _ = resp.Write(bytes)
	}
	return
ERR:
	// 异常应答
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err == nil {
		_, _ = resp.Write(bytes)
	}
}

// 杀死任务
// POST /job/kill name=job1
func handleJobKill(resp http.ResponseWriter,req *http.Request){
	var (
		err error
		name string
		bytes []byte
	)
	// 解析post表单
	if err = req.ParseForm();err != nil {
		goto ERR
	}
	// 要杀死的任务名
	name = req.PostForm.Get("name")
	// 杀死任务
	if err = G_jobMgr.KillJob(name);err != nil {
		goto ERR
	}
	// 正常应答
	if bytes,err = common.BuildResponse(0,"success",nil);err == nil {
		//fmt.Printf("删除后返回旧值：%s\n",string(bytes))
		_, _ = resp.Write(bytes)
	}
	return
ERR:
	// 异常应答
	if bytes,err = common.BuildResponse(-1,err.Error(),nil);err == nil {
		_, _ = resp.Write(bytes)
	}
}



// 初始化服务
func InitApiServer()(err error){
	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir // 静态文件根目录
		staticHandler http.Handler// 静态文件的HTTP回调
	)
	// 配置路由
	mux = http.NewServeMux()
	// job保存接口
	mux.HandleFunc("/job/save",handleJobSave)
	// job删除接口
	mux.HandleFunc("/job/delete",handleJobDelete)
	// job查看接口
	mux.HandleFunc("/job/list",handleJobList)
	// 杀死任务
	mux.HandleFunc("/job/kill",handleJobKill)

	//静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticHandler)) // ./webroot/index.html

	// 启动TCP监听
	if listener,err = net.Listen("tcp",":" + strconv.Itoa(G_config.ApiPort));err != nil {
		return
	}

	// 创建一个http服务
	httpServer = &http.Server{
		ReadTimeout: time.Duration(G_config.ApiReadTimeOut) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeOut) * time.Millisecond,
		Handler: mux,
	}

	//赋值单例
	G_apiServer = &ApiServer{
		httpServer:httpServer,
	}

	// 启动了服务端
	go httpServer.Serve(listener)


	return
}
