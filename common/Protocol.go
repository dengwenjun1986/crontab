package common

import "encoding/json"

type Job struct {
	Name string `json:"name"` // 任务名
	Command string `json:"command"` // shell命令
	CronExpr string 	`json:"cronexpr"` // 表达式
}
// HTTP接口应答
type Response struct {
	Errno int `json:"errno"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

// 应答方法
func BuildResponse(errno int,msg string,data interface{})(resp []byte,err error)  {
	// 定义一个response
	var (
		response Response
	)
	errno = response.Errno
	msg = response.Msg
	data = response.Data

	// 序列化json
	resp,err = json.Marshal(response)

	return
}
