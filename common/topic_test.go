package common_test

import (
	"fdqtest/common"
	"testing"
)

func TestCreateTopic(t *testing.T) {
	common.CreateTopic("ceshifd", "127.0.0.1:2121")
}