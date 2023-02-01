package main

import (
	"flag"
	"fmt"
	"github.com/dengwenjun1986/crontab/master"
	"runtime"
	"time"
)

var (
	configfile string
)
func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}
// 命令行传入配置文件
func initArgs(){
	flag.StringVar(&configfile,"config","./config.json","指定config.json")
	flag.Parse()
}
func main() {
	var (
		err error
	)
	// 初始化线程
	initEnv()
	//初始化参数文件
	initArgs()

	// 加载配置文件
	if err = master.InitConfig(configfile);err != nil {
		goto ERR
	}

	// 任务管理器
	if err = master.InitJobMgr();err != nil {
		goto ERR
	}

	// 启动Api HTTP服务
	if err = master.InitApiServer();err != nil{
		goto ERR
	}
	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}
	//return
ERR:
	fmt.Println(err)
}
