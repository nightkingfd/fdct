package main

import (
	"fdqtest/common"
	"fmt"
	"fdqtest/fdserver"
)

func main() {
	err := Run()
	CheckErr(err)
}

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func Run() error {
	//获取ct配置
	config, err := common.GetServerConfig()
	if err != nil {
		return err
	}
	server := fdserver.GetServer()

	for _, v := range config {
		temp := v.(map[string]interface{})
		//注册当前server服务
		//topic初始化
		topic := server.Connect(temp["topic"].(string), temp["server_addr"].(string))
		fdserver.CtStart(temp["ct"].([]interface{}), topic)
	}
	select {} //阻塞主进程不退出
}