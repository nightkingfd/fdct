package common

import (
	"fmt"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"time"
	"strconv"
	"os/exec"
	"github.com/go-simplejson"
)


type Task struct {
	TaskName string
	Topic    *Topic
}

func (ta *Task) CreateTask(data []byte) error {
	t := time.Now()
	taskNode := fmt.Sprintf("/%s/%s-%d-%d-%d", ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	ok, _, _ := ta.Topic.Zk.Conn.Exists(taskNode)
	if !ok {
		err := ta.Topic.Zk.CreateNode(taskNode, data, 0, zk2.WorldACL(zk2.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}

func (ta *Task) SendTask(data []byte) (seq int64, err error) {
	t := time.Now()
	parentTaskNode := fmt.Sprintf("/%s/%s-%d-%d-%d", ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	ok, _, err := ta.Topic.Zk.Conn.Exists(parentTaskNode)
	if err != nil {
		return 0, err
	}
	if !ok {
		ctime := t.Unix()
		err = ta.CreateTask([]byte(strconv.FormatInt(ctime,10)))
		if err != nil {
			return 0, err
		}
	}
	taskNode := fmt.Sprintf("%s/", parentTaskNode)
	_, err = ta.Topic.Zk.Conn.Create(taskNode, data, zk2.FlagSequence, zk2.WorldACL(zk2.PermAll))
	return 0, err
}


func (ta *Task) ListenNode() error {
	t := time.Now()
	node := fmt.Sprintf("/%s/%s-%d-%d-%d", ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	ta.dealFirstTaskList(node)
WATCH:
	nodes, _, event, err := ta.Topic.Zk.Conn.ChildrenW(node)
	fmt.Println("listen...")
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	<-event
	//获取节点
	nodes, _, _ = ta.Topic.Zk.GetNodeChildren(node)
	ta.dealTask(node, nodes)
	goto WATCH
}

func (ta *Task) getTask(taskNode string) ([]byte, error) {
	content, _, err := ta.Topic.Zk.GetNode(taskNode)
	return content,err
}

func (ta *Task) dealTask(node string, nodes []string)  {
	for _, v := range nodes {
		taskNode := fmt.Sprintf("%s/%s", node, v)
		task ,err := ta.getTask(taskNode)
		ta.Topic.Zk.DelNode(taskNode)
		if err != nil {
			//todo write log
			continue
		}
		fmt.Println("task content:", string(task))
		js , err := simplejson.NewJson(task)
		if err != nil {
			//todo write log
			continue
		}
		SafeRun(js.Get("CmdBin").MustString(), js.Get("CmdFile").MustString(), js.Get("CmdParam").MustString())
	}
}

func (ta *Task) dealFirstTaskList(node string) error {
	nodes, _, err := ta.Topic.Zk.GetNodeChildren(node)
	if err != nil {
		return err
	}
	ta.dealTask(node, nodes)
	return nil
}

func SafeRun(cmd string, path string, param string) {
	fmt.Println(cmd, path, param)
	timeout := TIMEOUT
	ch := make(chan int, 1)
	var out []byte
	go func(out []byte) {
		defer func() {
			if err := recover(); err != nil {
				//todo write log
			}
		}()
		out, err := exec.Command(cmd, path, param).Output()
		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}
		ch <- 1
	}(out)
	select {
	case <-time.After(timeout):
		fmt.Println("timeout")
		return
	case <-ch:
		fmt.Printf("\r\nresult:%s\r\n", out)
		return
	}
}