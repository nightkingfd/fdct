package fdserver

import (
	"errors"
	"fdqtest/common"
	"fmt"
	zk2 "github.com/samuel/go-zookeeper/zk"
)

type Server struct {
	Ip     string
	Topics []*common.Topic
}

func GetServer() *Server {
	ip, err := common.GetIp()
	if err != nil {
		panic(err)
	}
	return &Server{
		Ip: ip,
	}
}

func (cl *Server) Connect(topicName string, addr string) *common.Topic {
	topic, err := common.CreateTopic(topicName, addr)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	err = cl.JoinNode(topic)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	cl.Topics = append(cl.Topics, topic)
	return topic
}

func (cl *Server) JoinNode(tp *common.Topic) error {
	node := fmt.Sprintf(common.ServerNode, tp.Name)
	ok, _, err := tp.Zk.Conn.Exists(node)
	if !ok {
		fmt.Println(err.Error())
		return errors.New("no server parent node")
	}
	node = fmt.Sprintf("%s/server-", node)
	_,err = tp.Zk.CreateNode(node, []byte(cl.Ip), zk2.FlagSequence|zk2.FlagEphemeral, zk2.WorldACL(zk2.PermAll))
	return err
}

