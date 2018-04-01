package fdserver

import (
	"errors"
	"fmt"
	"github.com/go-simplejson"
	"github.com/robfig/cron"
	"io/ioutil"
	"fdqtest/common"
	"runtime/debug"
	"encoding/json"
)

/**
ct配置维护
*/

type CtInterface interface {
	ReadCt() ([]Cmd, error)
	CheckCt(*simplejson.Json) bool
}

type Ct struct {
	engine string //ct 存储引擎
	path   string //物理存储位置
}

type ZkCt struct {
	Ct
}

type FileCt struct {
	Ct
}

type Cmd struct {
	CmdSchedule string
	CmdBin      string
	CmdParam    string
	CmdFile     string
	CmdName		string
}

func GetIns(engine string, path string) (interface{}, error) {
	switch engine {
	case "zk":
		ins := &ZkCt{}
		ins.engine = "zk"
		return ins, nil
	case "file":
		ins := &FileCt{}
		ins.engine = "file"
		ins.path = path
		return ins, nil
	}
	return nil, errors.New("no support engine")
}

func (ct *Ct) CheckCt(json *simplejson.Json) bool {
	return true
}

func (ct *ZkCt) CheckCt(json *simplejson.Json) bool {
	return true
}

func (ct *FileCt) CheckCt(json *simplejson.Json) bool {
	return true
}

func (ct *Ct) ReadCt() (cmd []Cmd, err error) {
	return cmd, err
}

func (ct *ZkCt) ReadCt() (cmd []Cmd, err error) {
	return cmd, err
}

//读所有ct数据
func (ct *FileCt) ReadCt() ([]Cmd, error) {
	content, err := ioutil.ReadFile(ct.path)
	if err != nil {
		return nil, err
	}
	js, err := ct.formatJson(content)
	if err != nil {
		return nil, err
	}

	if !ct.CheckCt(js) {
		return nil, fmt.Errorf("error:invalid ct config")
	}

	var res []Cmd
	jsArr, err := js.Array()
	for _, v := range jsArr {
		var tempJs = v.(map[string]interface{})
		cmd := Cmd{
			CmdSchedule: tempJs["cmd_schedule"].(string),
			CmdBin:      tempJs["cmd_bin"].(string),
			CmdParam:    tempJs["cmd_param"].(string),
			CmdFile:     tempJs["cmd_file"].(string),
			CmdName:     tempJs["cmd_name"].(string),
		}
		res = append(res, cmd)
	}
	return res, nil
}

func (ct *FileCt) formatJson(content []byte) (*simplejson.Json, error) {
	json, err := simplejson.NewJson([]byte(content))
	if err != nil {
		return nil, err
	}
	return json, nil
}

func CtStart(cmd []interface{}, topic *common.Topic) {
	ct := cron.New()
	for _, v := range cmd {
		temp := v.(map[string]interface{})
		ct.AddFunc(temp["cmd_schedule"].(string), func() {
			defer func() {
				if err := recover(); err != nil {
					fmt.Printf("[ct task] recovering reason is %+v. More detail:", err)
					fmt.Println(string(debug.Stack()))
				}
			}()
			fmt.Println("ct start")
			fmt.Printf("cmdbin:%s,cmdpath:%s,cmdparam:%s\r\n", temp["cmd_bin"].(string), temp["cmd_file"].(string),
				temp["cmd_param"].(string))
			//发送命令点
			//cmdBin := []byte(temp["cmd_bin"].(string))
			task := common.Task{
				TaskName: temp["cmd_name"].(string),
				Topic:    topic,
			}
			cmdBin := Cmd{
				CmdBin:temp["cmd_bin"].(string),
				CmdSchedule:temp["cmd_schedule"].(string),
				CmdParam:temp["cmd_param"].(string),
				CmdFile:temp["cmd_file"].(string),
				CmdName:temp["cmd_name"].(string),
			}
			cmdByteBin, err  := json.Marshal(cmdBin)
			if err != nil {
				panic(err)
			}
			err = task.CreateTask(cmdByteBin)
			if err != nil {
				panic(err)
			}
			_, err = task.SendTask(cmdByteBin)
			if err != nil {
				panic(err)
			}
		})
	}
	ct.Start()
}
