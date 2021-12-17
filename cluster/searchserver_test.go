package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/awesomefly/easysearch/config"
	"github.com/awesomefly/easysearch/index"
	"github.com/stretchr/testify/assert"
)

func TestSearchServer(t *testing.T) {

	var managerConfig = config.Config{
		Server: config.Server{
			Host: "127.0.0.1",
			Port: 1234,
		},
		Cluster: config.Cluster{
			ShardingNum:  10,
			ReplicateNum: 3,
		},
	}

	var dataSvrConfig = config.Config{
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

	var srhSvrConfig = config.Config{
		Server: config.Server{
			Host: "127.0.0.1",
			Port: 1235,
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

	//start ManagerServer
	ms := NewManagerServer(&managerConfig)
	assert.NotNil(t, ms)
	go ms.Run()
	time.Sleep(1 * time.Second)

	//start DataServer

	ds := NewDataServer(&dataSvrConfig)
	assert.NotNil(t, ds)
	go ds.Run()
	time.Sleep(1 * time.Second)

	srh := NewSearchServer(&srhSvrConfig)
	var response []index.Doc
	err := srh.SearchAll("Jordan", &response)
	assert.Nil(t, err)

	fmt.Printf("%+v\n", response)
}
