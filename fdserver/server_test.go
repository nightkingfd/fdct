package fdserver

import (
	"testing"
	"fdqtest/common"
	"fmt"
)

func TestGetServer(t *testing.T) {
	serv := GetServer()
	config,err:=common.GetServerConfig()
	if err != nil {
		panic(err)
	}
	for _,v:=range(config)  {
		fmt.Println(v)
		temp := v.(map[string]interface{})
		serv.Connect(temp["topic"].(string), temp["server_addr"].(string))
	}
}