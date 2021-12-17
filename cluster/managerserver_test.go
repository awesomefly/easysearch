package cluster

import (
	"fmt"
	"log"
	"net/rpc"
	"runtime"
	"testing"
	"time"

	"github.com/awesomefly/easysearch/config"
	"github.com/stretchr/testify/assert"
)

func addServer(host string) error {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
		return err
	}
	var request = Node{
		Host: host,
	}
	var response Node
	err = client.Call("ManagerServer.AddServer", request, &response)
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Printf("resp:%+v\n", response)
	client.Close()
	return nil
}

func getCluster() error {
	client, err := rpc.Dial("tcp", ":1234")
	if err != nil {
		log.Fatal("dialing:", err)
		return err
	}

	var response Cluster
	err = client.Call("ManagerServer.GetCluster", "local", &response)
	if err != nil {
		log.Fatal(err)
		return err
	}

	fmt.Printf("resp:%+v\n", response)
	client.Close()
	return nil
}

func TestManageServer(t *testing.T) {
	conf := config.Config{
		Server: config.Server{
			Host: "127.0.0.1",
			Port: 1234,
		},

		Cluster: config.Cluster{
			ShardingNum:  10,
			ReplicateNum: 3,
		},
	}
	server := NewManagerServer(&conf)
	go server.Run()
	runtime.Gosched()

	time.Sleep(1 * time.Second)
	assert.NotNil(t, server)

	assert.Equal(t, nil, addServer("127.0.0.1:8801"))
	assert.Equal(t, nil, getCluster())
	assert.Equal(t, nil, addServer("127.0.0.1:8802"))
	assert.Equal(t, nil, getCluster())
	assert.Equal(t, nil, addServer("127.0.0.1:8803"))
	assert.Equal(t, nil, getCluster())
	assert.Equal(t, nil, addServer("127.0.0.1:8804"))
	assert.Equal(t, nil, getCluster())
}
