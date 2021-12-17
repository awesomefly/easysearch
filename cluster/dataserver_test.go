package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/awesomefly/simplefts/config"
	"github.com/awesomefly/simplefts/index"
	"github.com/stretchr/testify/assert"
)

func TestDataServer(t *testing.T) {
	managerConfig := config.Config{
		Server: config.Server{
			Host: "127.0.0.1",
			Port: 1234,
		},
		Cluster: config.Cluster{
			ShardingNum:  10,
			ReplicateNum: 3,
		},
	}
	server := NewManagerServer(&managerConfig)
	assert.NotNil(t, server)
	go server.Run()
	time.Sleep(1 * time.Second)

	dataSvrConfig := config.Config{
		Store: config.Storage{
			IndexFile: "../data/wiki_index",
		},
		Server: config.Server{
			Host: "127.0.0.1",
			Port: 1240,
		},
		Cluster: config.Cluster{
			ShardingNum:  10,
			ReplicateNum: 3,
			ManageServer: config.Server{
				Host: "127.0.0.1",
				Port: 1234,
			},
		},
	}
	ds := NewDataServer(&dataSvrConfig)
	assert.NotNil(t, ds)

	var response []index.Doc
	err := ds.Search(SearchRequest{Query: "Jordan", Sharding: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}, &response)
	assert.Nil(t, err)

	fmt.Printf("%+v\n", response)
}
