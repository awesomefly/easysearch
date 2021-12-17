package cluster

import (
	"log"
	"net/rpc"

	"github.com/awesomefly/simplefts/util"

	"github.com/awesomefly/simplefts/config"
	"github.com/awesomefly/simplefts/index"
)

//RpcCall RPC方法必须满足Go语言的RPC规则：方法只能有两个可序列化的参数，其中第二个参数是指针类型，并且返回一个error类型，同时必须是公开的方法
func RpcCall(host string, method string, request interface{}, response interface{}) error {
	client, err := rpc.Dial("tcp", host)
	if err != nil {
		log.Fatal("dialing:", err)
		return err
	}

	switch v := response.(type) {
	case *Node:
		err = client.Call(method, request, v)
	case *Cluster:
		err = client.Call(method, request, v)
	case *[]index.Doc:
		err = client.Call(method, request, v)
	}
	if err != nil {
		log.Fatal(err)
		return err
	}
	client.Close()

	log.Printf("RPC Response:%+v", response)
	return nil
}

type SearchClient struct {
	ServerConfig *config.Server //manager server config
	cluster      *Cluster       //todo: cached and refresh cluster info
}

func NewSearchClient(config *config.Server) *SearchClient {
	client := SearchClient{
		ServerConfig: config,
		cluster:      &Cluster{},
	}

	err := RpcCall(client.ServerConfig.Address(), "ManagerServer.GetCluster", util.GetLocalIP(), &client.cluster)
	if err != nil {
		panic(err)
	}
	return &client
}

func (c *SearchClient) Search(query string) ([]index.Doc, error) {
	response := make([]index.Doc, 0)
	if err := RpcCall(c.cluster.RouteSearchNode().Host, "SearchServer.SearchAll", query, &response); err != nil {
		return response, err
	}
	return response, nil
}
