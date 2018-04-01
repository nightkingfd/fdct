package common_test

import (
	"fdqtest/common"
	"testing"
)

func TestZkConnect(t *testing.T) {
	common.DefaultZk("127.0.0.1:2121")
}
