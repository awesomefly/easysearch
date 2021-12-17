package cluster

import (
	"errors"
	"math/rand"
)

const (
	ManagerNode = 1
	DataNode    = 2
	SearchNode  = 3
)

type Node struct {
	ID   int
	Type int
	Host string //ip:port

	LeaderSharding   []int //主分片
	FollowerSharding []int //备份分片
}

type Cluster struct {
	ShardingNum  int //分片数
	ReplicateNum int //数据备份数

	SearchNodeCorpus []Node
	DataNodeCorpus   map[string]Node
}

func NewCluster(shard, replicate int) *Cluster {
	return &Cluster{
		ShardingNum:      shard,
		ReplicateNum:     replicate,
		SearchNodeCorpus: make([]Node, 0),
		DataNodeCorpus:   make(map[string]Node, 0),
	}
}

func (c *Cluster) Add(node Node) error {
	switch node.Type {
	case DataNode:
		c.DataNodeCorpus[node.Host] = node
	case SearchNode:
		c.SearchNodeCorpus = append(c.SearchNodeCorpus, node)
	default:
		return errors.New("invalid node type")
	}
	return nil
}

const (
	LeaderSharding   = 1
	FollowerSharding = 2
)

type Sharding2Node map[int][]Node

func (c *Cluster) RouteShardingNode(flag int) (Sharding2Node, error) {
	result := make(Sharding2Node, 0)
	for _, node := range c.DataNodeCorpus {
		switch flag {
		case LeaderSharding:
			for _, shard := range node.LeaderSharding {
				result[shard] = append(result[shard], node)
			}
		case FollowerSharding:
			for _, shard := range node.FollowerSharding {
				result[shard] = append(result[shard], node)
			}
		}
	}
	return result, nil
}

func (c *Cluster) RouteSearchNode() Node {
	n := rand.Intn(len(c.SearchNodeCorpus))
	return c.SearchNodeCorpus[n]
}
