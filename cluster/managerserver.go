package cluster

import (
	"errors"
	"fmt"
	"log"

	"github.com/awesomefly/simplefts/util"

	"github.com/serialx/hashring"

	"github.com/awesomefly/simplefts/config"
)

type ManagerServer struct {
	cluster *Cluster
	hash    *hashring.HashRing

	server *Server
}

func NewManagerServer(config *config.Config) *ManagerServer {
	srv := &ManagerServer{
		cluster: NewCluster(config.Cluster.ShardingNum, config.Cluster.ReplicateNum),
		hash:    hashring.New(make([]string, 0)),
		server:  &Server{name: "Manage", network: "tcp", address: config.Server.Address()},
	}
	return srv
}

func (m *ManagerServer) Run() {
	if err := m.server.RegisterName("ManagerServer", m); err != nil {
		panic(err)
	}
	if err := m.server.Run(); err != nil {
		panic(err)
	}
}

// AddServer called by SearchServer
func (m *ManagerServer) AddServer(request Node, response *Node) error {
	log.Print("AddServer from ", request.Host)

	m.cluster.Add(request)
	if request.Type == DataNode {
		m.hash = m.hash.AddNode(request.Host)
		if err := m.ReBalance(); err != nil {
			return err
		}

		go func() {
			//todo:使用channel通知分片信息有变化的节点
		}()

		*response = m.cluster.DataNodeCorpus[request.Host]
	}
	return nil
}

// GetCluster called by DataServer
func (m *ManagerServer) GetCluster(request string, response *Cluster) error {
	log.Print("GetCluster from ", request)
	*response = *m.cluster
	return nil
}

func (m *ManagerServer) ReBalance() error {
	for k, node := range m.cluster.DataNodeCorpus {
		node.LeaderSharding = make([]int, 0)
		node.FollowerSharding = make([]int, 0)
		m.cluster.DataNodeCorpus[k] = node
	}

	size := util.IfElseInt(len(m.cluster.DataNodeCorpus) < m.cluster.ReplicateNum, len(m.cluster.DataNodeCorpus), m.cluster.ReplicateNum)
	for i := 0; i < m.cluster.ShardingNum; i++ {
		nodes, ok := m.hash.GetNodes(fmt.Sprint(i), size)
		if !ok {
			return errors.New("get nodes err: invalid replicated num ")
		}
		if len(nodes) < size {
			return errors.New("unexpected nodes size err. ")
		}

		n := m.cluster.DataNodeCorpus[nodes[0]]
		n.LeaderSharding = append(n.LeaderSharding, i)
		m.cluster.DataNodeCorpus[n.Host] = n
		for _, k := range nodes[1:] {
			n = m.cluster.DataNodeCorpus[k]
			n.FollowerSharding = append(n.FollowerSharding, i)
			m.cluster.DataNodeCorpus[n.Host] = n
		}

	}
	return nil
}
