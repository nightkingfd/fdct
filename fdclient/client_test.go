package fdclient_test

import (
	"fdqtest/fdclient"
	"testing"
)

func TestClient_Connect(t *testing.T) {
	cl := &fdclient.Client{}
	taskName := []string{"ceshi", "dec"}
	iteaskName := make([]interface{}, len(taskName))
	for k, v:=range(taskName) {
		iteaskName[k] = v
	}
	cl.Connect("ceshifd", "127.0.0.1:2121", iteaskName)
}
