package config

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type BM25Parameters struct {
	K1 float32 `yaml:"K1"`
	B  float32 `yaml:"B"`
}

type Storage struct {
	DumpFile  string `yaml:"DumpFile"`
	IndexFile string `yaml:"IndexFile"`
}

type Cluster struct {
	ShardingNum  int      `yaml:"ShardingNum"`
	ReplicateNum int      `yaml:"ReplicateNum"`
	ManageServer Server   `yaml:"ManageServer"`
	SearchServer []Server `yaml:"SearchServer"`
	DataServer   []Server `yaml:"DataServer"`
}

type Server struct {
	Host string `yaml:"Host"`
	Port int    `yaml:"Port"`
}

func (s *Server) Address() string {
	return fmt.Sprint(s.Host, ":", s.Port)
}

type Config struct {
	Store   Storage        `yaml:"Storage"`
	BM25    BM25Parameters `yaml:"BM25"`
	Server  Server         `yaml:"Server"`
	Cluster Cluster        `yaml:"Cluster"`
}

func InitClusterConfig(path string) *Cluster {
	file, _ := filepath.Abs(path)
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err.Error())
	}

	cluster := Cluster{}
	if err = yaml.Unmarshal(buffer, &cluster); err != nil {
		panic(err.Error())
	}
	fmt.Printf("cluster: %+v\n", cluster)
	return &cluster
}

func InitConfig(path string) *Config {
	file, _ := filepath.Abs(path)
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err.Error())
	}

	config := Config{}
	if err = yaml.Unmarshal(buffer, &config); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("config: %+v\n", config)
	return &config
}
