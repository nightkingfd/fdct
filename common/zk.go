package common

import (
	"errors"
	"fmt"
	zk2 "github.com/samuel/go-zookeeper/zk"
	"sync"
	"time"
)

const TIMEOUT = 4 * time.Second

type Zk struct {
	Conn *zk2.Conn
	lock sync.Mutex
}

func DefaultZk(addr string) *Zk {
	var defaultZk *Zk
	zkConn, err := zkConnect(addr)
	if err != nil {
		panic(err)
	}
	defaultZk = &Zk{
		Conn: zkConn,
	}
	return defaultZk
}

func zkConnect(addr string) (*zk2.Conn, error) {
	option := zk2.WithEventCallback(callback)
	conn, ch, err := zk2.Connect([]string{addr}, TIMEOUT, option)
	if err != nil {
		return nil, err
	}
	var isConn = false
	var timeout chan bool

	go func(ch chan bool) {
		time.Sleep(TIMEOUT)
		ch <- true
	}(timeout)
	select {
	case connEvent := <-ch:
		if connEvent.State == zk2.StateConnecting {
			isConn = true
			fmt.Println("connect to zookeeper server success!")
		} else {
			fmt.Println("connect zk fail")
		}
	case <-timeout: //<-time.After(time.Second * 3)
		isConn = false
	}
	if !isConn {
		return nil, errors.New("connect zk fail")
	}
	return conn, nil
}

func callback(event zk2.Event) {
	//fmt.Println("*******************")
	//fmt.Println("path:", event.Path)
	//fmt.Println("type:", event.Type.String())
	//fmt.Println("state:", event.State.String())
	//fmt.Println("-------------------")
}

func (zk *Zk) Lock() {
	zk.lock.Lock()
}

func (zk *Zk) Unlock() {
	zk.lock.Unlock()
}

func (zk *Zk) GetNodeChildren(node string) ([]string, *zk2.Stat, error) {
	zk.Conn.Sync(node)
	return zk.Conn.Children(node)
}

func (zk *Zk) GetNode(node string) ([]byte, *zk2.Stat, error) {
	zk.Conn.Sync(node)
	return zk.Conn.Get(node)
}

//创建节点
func (zk *Zk) CreateNode(node string, data []byte, flags int32, acl []zk2.ACL) error {
	ok, _, err := zk.Conn.Exists(node)
	if err != nil {
		return err
	}
	if !ok {
		_, err := zk.Conn.Create(node, data, flags, acl)
		if err != nil {
			return err
		}
	}
	return nil
}

//删除节点
func (zk *Zk) DelNode(node string) error {
	ok, stat, err := zk.Conn.Exists(node)
	if err != nil {
		return err
	}
	if ok {
		zk.Conn.Delete(node, stat.Version)
	}
	return nil
}
