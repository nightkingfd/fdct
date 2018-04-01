package fdserver

import (
	"errors"
	"fdqtest/common"
	"fmt"
	"net"

)

type Server struct {
	Ip     string
	Topics []*common.Topic
}

func GetServer() *Server {
	ip, err := getIp()
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
	err = topic.CreateClientNode()
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	cl.Topics = append(cl.Topics, topic)
	return topic
}

func getIp() (string, error) {
	addr, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addr {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("no ip")
}


