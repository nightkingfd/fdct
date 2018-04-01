package common

import (
	"fmt"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"time"
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
	taskNode := fmt.Sprintf("/%s", name)
	err := zk.CreateNode(taskNode, []byte(timeSf), 0, zk2.WorldACL(zk2.PermAll))
	if err != nil {
		return nil, err
	}
	return topic, nil
}

func (tp *Topic) CreateClientNode() error {
	node := fmt.Sprintf("/%s/client", tp.Name)
	ok, _, _ := tp.Zk.Conn.Exists(node)
	if !ok {
		err := tp.Zk.CreateNode(node, nil, zk2.FlagSequence|zk2.FlagEphemeral, zk2.WorldACL(zk2.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}