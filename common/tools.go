package common

import (
	"runtime"
	"fmt"
	"github.com/go-simplejson"
	"io/ioutil"
	path2 "path"
	"errors"
	"net"
)

func GetServerConfig() ([]interface{}, error) {
	_, filename, _, _ := runtime.Caller(0)
	path := fmt.Sprintf("%s/../config/server.ini", path2.Dir(filename))
	fmt.Println(path)
	content, err := ioutil.ReadFile(path)
	js, err := simplejson.NewJson(content)
	if err != nil {
		return nil, err
	}
	jsArr, err := js.Array()
	return jsArr, err
}


func GetIp() (string, error) {
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