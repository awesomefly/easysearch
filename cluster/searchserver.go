package cluster

import (
	"math/rand"
	"sort"

	"github.com/awesomefly/easysearch/config"

	"github.com/awesomefly/easysearch/index"
)

type SearchServer struct {
	cluster Cluster
	server  *Server
}

func NewSearchServer(config *config.Config) *SearchServer {
	self := Node{
		ID:   rand.Intn(10000), //todo: support uuid
		Type: SearchNode,
		Host: config.Server.Address(),
	}
	err := RpcCall(config.Cluster.ManageServer.Address(), "ManagerServer.AddServer", self, &Node{})
	if err != nil {
		panic(err)
	}

	c := Cluster{}
	err = RpcCall(config.Cluster.ManageServer.Address(), "ManagerServer.GetCluster", "", &c)
	if err != nil {
		panic(err)
	}

	return &SearchServer{
		cluster: c,
		server:  &Server{name: "Search", network: "tcp", address: config.Server.Address()},
	}

	return nil
}

func (s *SearchServer) Run() {
	if err := s.server.RegisterName("SearchServer", s); err != nil {
		panic(err)
	}
	if err := s.server.Run(); err != nil {
		panic(err)
	}
}

//SearchAll 分布式搜索
//todo: 实时更新&删除
func (s *SearchServer) SearchAll(query string, response *[]index.Doc) error {
	r, err := s.cluster.RouteShardingNode(FollowerSharding) //todo: cache router info
	if err != nil {
		return err
	}

	if r == nil || len(r) == 0 {
		if r, err = s.cluster.RouteShardingNode(LeaderSharding); err != nil {
			return err
		}
	}

	result := make([]index.Doc, 0)
	for sharding, nodes := range r {
		n := rand.Intn(len(nodes))

		request := SearchRequest{
			Query:    query,
			Sharding: []int{sharding},
		}
		var reply []index.Doc
		if err = RpcCall(nodes[n].Host, "DataServer.Search", request, &reply); err != nil {
			return err
		}
		result = append(result, reply...)
	}

	//sort and uniq result
	sort.Slice(result, func(i, j int) bool {
		return result[i].BM25 > result[j].BM25 //降序
	})
	*response = result
	return nil
}
