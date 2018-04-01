package main

import (
	"fmt"
	"github.com/go-simplejson"
	"io/ioutil"
	path2 "path"
	"runtime"
	"fdqtest/fdclient"
)

func main()  {
	//todo 清除topic所有任务
	//todo 清除topic task 所有任务
	//todo 热重启

	ClientRun()
}

func ClientRun() {
	//初始化 topic connect
	_, filename, _, _ := runtime.Caller(0)
	path := fmt.Sprintf("%s/config/client.ini", path2.Dir(filename))
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	js, err := simplejson.NewJson([]byte(content))
	if err != nil {
		panic(err)
	}
	jsArr, err := js.Array()
	if err != nil {
		panic(err)
	}
	cl := fdclient.GetClient()
	for _, v := range jsArr {
		temp := v.(map[string]interface{})
		tempS := temp["task"].([]interface{})
		cl.Connect(temp["topic"].(string), temp["server_addr"].(string), tempS)
	}
	for _, topic := range cl.Topics {
		temp := topic.Task
		for _,v:=range(temp) {
			go v.ListenNode()
		}
	}
	select {}
}
