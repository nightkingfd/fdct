package common

import (
	"fmt"
	"github.com/go-simplejson"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"os/exec"
	"strconv"
	"time"
)

const (
	TaskNode = "/%s/%s-%d-%d-%d"
	TaskParentLockNode = "/%s/%s"
	TaskLockNode = "/%s/%s/client-lock"
)


type Task struct {
	TaskName string
	Topic    *Topic
	seq uint64 //当前task seq
	count uint64 //已执行次数
	ConsumeTaskLimit uint8 //限制消费次数
	PauseChan chan struct{}
	ResumeChan chan struct{}
}

func (ta *Task) CreateTask(data []byte) error {
	t := time.Now()
	taskNode := fmt.Sprintf(TaskNode, ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	_,err := ta.Topic.Zk.CreateNode(taskNode, data, 0, zk2.WorldACL(zk2.PermAll))
	//创建task lock parent node
	lockNode := fmt.Sprintf(TaskParentLockNode, ta.Topic.Name, ta.TaskName)
	ta.Topic.Zk.CreateNode(lockNode, nil, 0, zk2.WorldACL(zk2.PermAll))
	//创建task lock
	lockNode = fmt.Sprintf(TaskLockNode, ta.Topic.Name, ta.TaskName)
	ta.Topic.Zk.CreateNode(lockNode, nil, 0, zk2.WorldACL(zk2.PermAll))
	return err
}

func (ta *Task) SendTask(data []byte) (seq uint64, err error) {
	t := time.Now()
	parentTaskNode := fmt.Sprintf(TaskNode, ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	ok, _, err := ta.Topic.Zk.Conn.Exists(parentTaskNode)
	if err != nil {
		return 0, err
	}
	if !ok {
		createTime := t.Unix()
		err = ta.CreateTask([]byte(strconv.FormatInt(createTime, 10)))
		if err != nil {
			return 0, err
		}
	}
	taskNode := fmt.Sprintf("%s/", parentTaskNode)
	ta.seq++
	_, err = ta.Topic.Zk.Conn.Create(taskNode, data, zk2.FlagSequence, zk2.WorldACL(zk2.PermAll))
	return ta.seq, err
}

func (ta *Task) ListenNode(uuid string) error {
	t := time.Now()
	node := fmt.Sprintf(TaskNode, ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
	go ta.listenLock(uuid)
	ta.ResumeChan<- struct{}{}
PAUSE:
	fmt.Println("waiting")
	<-ta.ResumeChan
	if len(ta.PauseChan) > 0 {
		<-ta.PauseChan
	}
WATCH:
	nodes, _, event, err := ta.Topic.Zk.Conn.ChildrenW(node)
	fmt.Println("listen...")
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	select {
	case <-event:
		//获取节点
		nodes, _, _ = ta.Topic.Zk.GetNodeChildren(node)
		ta.dealTask(node, nodes)
		goto WATCH
	case <-ta.PauseChan:
		fmt.Println("wait")
		goto PAUSE

	}
}

func (ta *Task) getTask(taskNode string) ([]byte, error) {
	content, _, err := ta.Topic.Zk.GetNode(taskNode)
	return content, err
}

func (ta *Task) dealTask(node string, nodes []string) {
	for _, v := range nodes {
		taskNode := fmt.Sprintf("%s/%s", node, v)
		task, err := ta.getTask(taskNode)
		ta.Topic.Zk.DelNode(taskNode)
		if err != nil {
			//todo write log
			continue
		}
		fmt.Println("start deal task")
		js, err := simplejson.NewJson(task)
		if err != nil {
			//todo write log
			continue
		}
		ta.count++
		SafeRun(js.Get("CmdBin").MustString(), js.Get("CmdFile").MustString(), js.Get("CmdParam").MustString())
	}
	if ta.count >= uint64(ta.ConsumeTaskLimit) {
		ta.count = 0
		node := fmt.Sprintf(TaskLockNode + "/curr-client", ta.Topic.Name, ta.TaskName)
		ta.Topic.Zk.DelNode(node)
		fmt.Println("delete ======> ", node)
	}
}

func (ta *Task) dealFirstTaskList() error {
	t := time.Now()
	node := fmt.Sprintf(TaskNode, ta.Topic.Name, ta.TaskName, t.Year(), t.Month(), t.Day())
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
	go func() {
		var out []byte
		defer func() {
			if err := recover(); err != nil {
				//todo write log
			}
		}()
		out, err := exec.Command(cmd, path, param).CombinedOutput()
		fmt.Printf("\r\nresult:%s\r\n", out)
		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}
		ch <- 1
	}()
	select {
	case <-time.After(timeout):
		fmt.Println("timeout")
		return
	case <-ch:
		return
	}
}

func (ta *Task) listenLock(uuid string)  {
	ta.vote(uuid)
	node := fmt.Sprintf(TaskLockNode + "/curr-client", ta.Topic.Name, ta.TaskName)
	//监听选举
	go func() {
		for {
			_, _, ch, err := ta.Topic.Zk.Conn.ChildrenW(node)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			<-ch
			master, err := ta.vote(uuid)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				fmt.Printf("vote retry is %s", master)
			}
		}
	}()
}


func (ta *Task) vote(uuid string) ([]byte, error) {
	time.Sleep(1*time.Second)
	node := fmt.Sprintf(TaskLockNode + "/curr-client", ta.Topic.Name, ta.TaskName)
	_, err := ta.Topic.Zk.CreateLockNode(node, []byte(uuid), zk2.FlagEphemeral, zk2.WorldACL(zk2.PermAll))
	if err == zk2.ErrNodeExists {
		fmt.Println(err.Error(), node)
		fmt.Println("master is exists")
	} else if err != nil {
		return nil, err
	}
	content, _, _ := ta.Topic.Zk.GetNode(node)
	fmt.Println("lock is：", string(content), " uuid is:" , uuid)
	if string(content) == uuid {
		fmt.Println("get access lock")
		ta.ResumeChan<- struct{}{}
	} else {
		if len(ta.ResumeChan) >0 {
			<-ta.ResumeChan
		}
		fmt.Println("deny access lock")
		ta.PauseChan<- struct{}{}
	}
	return content, nil
}