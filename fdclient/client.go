package fdclient

import (
	"fdqtest/common"
	"fmt"
)

type Client struct {
	Ip     string
	Topics []*common.Topic
}

func GetClient() *Client {
	ip, err := common.GetIp()
	if err != nil {
		panic(err)
	}
	return &Client{
		Ip: ip,
	}
}

func (cl *Client) Connect(topicName string, addr string, taskNames []interface{}) {
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
	var task []*common.Task
	for _, v := range taskNames {
		temp := &common.Task{
			TaskName: v.(string),
			Topic:topic,
		}
		task = append(task, temp)
	}
	topic.Task = task
	cl.Topics = append(cl.Topics, topic)
}

