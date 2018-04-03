package common

import (
	"fmt"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"time"
)

const (
	ClientNode  = "/%s/client"
	ServerNode  = "/%s/server"
	TopicNode  = "/%s"
)

type Topic struct {
	Name       string //topic name
	Zk         *Zk
	ServerAddr string
	Task       []*Task
}

func CreateTopic(name string, addr string) (*Topic, error) {
	zk := DefaultZk(addr)
	topic := &Topic{
		Name:       name,
		Zk:         zk,
		ServerAddr: addr,
	}

	time := time.Now().Unix()
	timeSf := fmt.Sprintf("{\"time\":%d}", time)
	taskNode := fmt.Sprintf(TopicNode, name)
	//创建任务父节点
	_,err := zk.CreateNode(taskNode, []byte(timeSf), 0, zk2.WorldACL(zk2.PermAll))
	//创建客户端父节点
	clientNode := fmt.Sprintf(ClientNode, name)
	zk.CreateNode(clientNode, []byte(timeSf), 0, zk2.WorldACL(zk2.PermAll))
	//创建服务端父节点
	serverNode := fmt.Sprintf(ServerNode, name)
	zk.CreateNode(serverNode, []byte(timeSf), 0, zk2.WorldACL(zk2.PermAll))
	if err != nil {
		return nil, err
	}
	return topic, nil
}
