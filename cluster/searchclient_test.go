package cluster

import (
	"fmt"
	"testing"

	"github.com/awesomefly/easysearch/config"
	"github.com/stretchr/testify/assert"
)

func TestSearchClient(t *testing.T) {
	config := &config.Server{
		Host: "127.0.0.1",
		Port: 1234,
	}
	cli := NewSearchClient(config)
	result, err := cli.Search("Album Jordan")
	assert.Nil(t, err)
	assert.NotNil(t, result)
	fmt.Printf("result:%+v\n", result)

}
