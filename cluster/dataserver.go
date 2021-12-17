package cluster

import (
	"fmt"
	"math/rand"

	"github.com/awesomefly/easysearch/config"

	"github.com/awesomefly/easysearch/index"
	"github.com/awesomefly/easysearch/singleton"
)

type DataServer struct {
	self    Node
	cluster Cluster

	sharding map[int]*singleton.Searcher
	server   *Server
}

func NewDataServer(config *config.Config) *DataServer {
	ds := DataServer{
		self: Node{
			ID:   rand.Intn(10000), //todo: support uuid
			Type: DataNode,
			Host: config.Server.Address(),
		},
		server:   &Server{name: "Data", network: "tcp", address: config.Server.Address()},
		sharding: make(map[int]*singleton.Searcher, 0),
	}

	n := Node{}
	err := RpcCall(config.Cluster.ManageServer.Address(), "ManagerServer.AddServer", ds.self, &n)
	if err != nil {
		panic(err)
	}
	ds.self = n

	c := Cluster{}
	err = RpcCall(config.Cluster.ManageServer.Address(), "ManagerServer.GetCluster", ds.self.Host, &c)
	if err != nil {
		panic(err)
	}
	ds.cluster = c
	//fmt.Printf("DataServer:%+v\n", ds)

	if len(config.Store.IndexFile) == 0 {
		panic("index file is empty.")
	}

	for _, shard := range ds.self.LeaderSharding {
		searcher := singleton.NewSearcher(fmt.Sprintf("%s.%d", config.Store.IndexFile, shard))
		ds.sharding[shard] = searcher
	}

	for _, shard := range ds.self.FollowerSharding {
		searcher := singleton.NewSearcher(fmt.Sprintf("%s.%d", config.Store.IndexFile, shard))
		ds.sharding[shard] = searcher
	}
	return &ds
}

func (s *DataServer) Run() {
	if err := s.server.RegisterName("DataServer", s); err != nil {
		panic(err)
	}
	if err := s.server.Run(); err != nil {
		panic(err)
	}
}

type SearchRequest struct {
	Query    string
	Sharding []int
}

//Search 搜索
func (s *DataServer) Search(request SearchRequest, response *[]index.Doc) error {
	result := make([]index.Doc, 0)
	for _, shard := range request.Sharding {
		srh := s.sharding[shard]
		if srh == nil {
			continue
		}
		x := srh.Search(request.Query)
		result = append(result, x...)
	}
	*response = result
	return nil
}

// Add 实时更新
func (s *DataServer) Add(doc index.Document) {
	shard := doc.ID % s.cluster.ShardingNum
	srh := s.sharding[shard]
	srh.Add(doc)
}

// Del 实时删除
func (s *DataServer) Del(doc index.Document) {
	shard := doc.ID % s.cluster.ShardingNum
	srh := s.sharding[shard]
	srh.Del(doc)
}

//KeepAlive 备份分片与主分片保持心疼，一旦发现主分片宕机，发起选举
//todo: 是否由ManageServer节点负责
/*
func (s *DataServer) KeepAlive() {
	for _, shardId := range s.self.FollowerSharding {
		key, ok := s.cluster.consistentHash.GetNode(fmt.Sprintf("%d", shardId))
		if !ok {
			panic("")
		}
		ip := s.cluster.DataNodeCorpus[key].IP
		port := s.cluster.DataNodeCorpus[key].Port

		ok := KeepAlive(ip, port)
		if !ok {
			keys, ok := s.cluster.consistentHash.GetNodes(fmt.Sprintf("%d", shardId), s.cluster.ReplicateNum+1)
			if !ok {
				panic("")
			}
			fllowers := GetNodes(keys)
			StratElection(fllowers)
		}
	}
}
*/
