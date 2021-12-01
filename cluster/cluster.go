package cluster

import (
	"errors"
	"math/rand"
)

type Node struct {
	ID   int
	IP   string
	Port int
}

type Cluster struct {
	Shard     int //分片数
	Replicate int //数据备份数

	Leaders   []Node
	Followers [][]Node
}

const (
	O_RD_ONLY int = 0
	O_WR_ONLY int = 1
)

func (c Cluster) Router(id int, flag int) (interface{}, error) {
	switch flag {
	case O_RD_ONLY:
		followers := c.Followers[id%c.Shard] //todo: 可升级为一致性哈希
		return followers[rand.Int()%len(followers)], nil
	case O_WR_ONLY:
		return c.Leaders[id%c.Shard], nil
	}
	return nil, errors.New("binary.Write: invalid type ")
}
