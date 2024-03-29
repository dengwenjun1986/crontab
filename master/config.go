package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// 程序配置
type Config struct {
	ApiPort int `json:"apiPort"`
	ApiReadTimeOut int `json:"apiReadTimeout"`
	ApiWriteTimeOut int `json:"apiWriteTimeout"`
	EtcdEndPoint string `json:"etcdEndpoints"`
	EtcdDialTimeOut int `json:"etcdDialtimeout"`
	WebRoot string 	`json:"webroot"`
}

// 单例
var (
	G_config *Config
)


// 加载配置
func InitConfig(filename string)(err error) {
	var (
		content []byte
		conf Config
	)

	if content,err = ioutil.ReadFile(filename);err != nil {
		return
	}

	// json反序列化配置
	if err = json.Unmarshal(content,&conf);err != nil {
		return
	}

	// 赋值单例
	G_config = &conf
	fmt.Println(conf)
	return
}
