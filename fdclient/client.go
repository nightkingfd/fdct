package fdclient

import (
	"fdqtest/common"
	"fmt"
	"errors"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"github.com/satori/go.uuid"
)

type Client struct {
	Ip     string
	Topics []*common.Topic
	uuid   string //client id
}

func GetClient() *Client {
	ip, err := common.GetIp()
	if err != nil {
		panic(err)
	}
	uuid, _:= uuid.NewV4()
	return &Client{
		Ip: ip,
		uuid: uuid.String(),
	}
}

func (cl *Client) Connect(topicName string, addr string, taskNames []interface{}) {
	topic, err := common.CreateTopic(topicName, addr)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	err = cl.JoinClientNode(topic)
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	var task []*common.Task
	for _, v := range taskNames {
		temp := &common.Task{
			TaskName: v.(string),
			Topic:topic,
			ConsumeTaskLimit:3,
			PauseChan: make(chan struct{}, 1),
			ResumeChan: make(chan struct{}, 1),
		}
		task = append(task, temp)
	}
	topic.Task = task
	cl.Topics = append(cl.Topics, topic)
}

func (cl *Client) JoinClientNode(tp *common.Topic) error {
	node := fmt.Sprintf(common.ClientNode, tp.Name)
	ok, _, err := tp.Zk.Conn.Exists(node)
	if !ok {
		fmt.Println(err.Error())
		return errors.New("no client parent node")
	}
	node = fmt.Sprintf("%s/client-", node)
	data := fmt.Sprintf("%s|%s", cl.Ip, cl.uuid)
	_,err = tp.Zk.CreateNode(node, []byte(data), zk2.FlagSequence|zk2.FlagEphemeral, zk2.WorldACL(zk2.PermAll))
	return err
}

//listen node
func (cl *Client) ListenNode(task *common.Task)  {
	//获取监听权利  监听client lock node
	//监听task node
	task.ListenNode(cl.uuid)
}

