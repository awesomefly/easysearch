package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/awesomefly/easysearch/cluster"
	"github.com/awesomefly/easysearch/config"

	"log"
	"os"
	"time"

	"github.com/awesomefly/easysearch/index"
	"github.com/awesomefly/easysearch/singleton"
)

func startStandaloneCluster() error {
	conf := config.InitClusterConfig("./cluster.yml")
	procAttr := &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	}

	procs := make([]*os.Process, 0)

	//start manager server
	baseArgs := os.Args[0] + " -m cluster "
	argv := strings.Fields(baseArgs + "--servername=managerserver --host=" +
		conf.ManageServer.Host + " --port=" + fmt.Sprint(conf.ManageServer.Port))
	proc, err := os.StartProcess(os.Args[0], argv, procAttr)
	if err != nil {
		fmt.Println("start manager server process error:", err)
		return err
	}
	procs = append(procs, proc)
	time.Sleep(3 * time.Second)

	//start data server
	for i := 0; i < len(conf.DataServer); i++ {
		srv := conf.DataServer[i]

		argv = strings.Fields(baseArgs + "--servername=dataserver --host=" + srv.Host + " --port=" + fmt.Sprint(srv.Port))
		proc, err = os.StartProcess(os.Args[0], argv, procAttr)
		if err != nil {
			fmt.Println("start data server process error:", err)
			return err
		}
		procs = append(procs, proc)
	}
	time.Sleep(3 * time.Second)

	//start search server
	for i := 0; i < len(conf.SearchServer); i++ {
		srv := conf.SearchServer[i]

		argv = strings.Fields(baseArgs + "--servername=searchserver --host=" + srv.Host + " --port=" + fmt.Sprint(srv.Port))
		proc, err = os.StartProcess(os.Args[0], argv, procAttr)
		if err != nil {
			fmt.Println("start search server process error:", err)
			return err
		}
		procs = append(procs, proc)
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < len(procs); i++ {
		_, err = procs[i].Wait()
		if err != nil {
			fmt.Println("wait error:", err)
			return err
		}
	}
	return nil
}

func main() {
	/*f, _ := os.OpenFile("cpu.pprof", os.O_CREATE|os.O_RDWR, 0666)
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer f.Close()*/

	log.SetOutput(os.Stdout)

	log.Printf("args:%+v\n", os.Args)

	var module string
	flag.StringVar(&module, "m", "", "[indexer|searcher|merger|cluster]")

	//searcher
	var query, source string
	flag.StringVar(&query, "q", "Album Jordan", "search query")
	flag.StringVar(&source, "source", "", "[local|remote]")

	//indexer
	var sharding bool
	flag.BoolVar(&sharding, "sharding", false, "true|false")

	//merger
	var srcPath, dstPath string
	flag.StringVar(&srcPath, "f", "", "src index file")
	flag.StringVar(&dstPath, "t", "", "dst index file")

	//server
	var servername string
	flag.StringVar(&servername, "servername", "", "[all|managerserver|dataserver|searchserver]")

	var host string
	var port int
	flag.StringVar(&host, "host", "", "server host")
	flag.IntVar(&port, "port", 0, "server port")
	flag.Parse()

	conf := config.InitConfig("./config.yml")
	if module == "indexer" {
		log.Println("Starting Index ...")
		if sharding {
			cluster.Index(conf)
		} else {
			singleton.Index(*conf) //todo: 构建索引耗时过长，性能分析下具体耗时原因
		}
	} else if module == "searcher" {
		log.Println("Starting local simple fts..")
		docs, err := index.LoadDocuments(conf.Store.DumpFile)
		if err != nil {
			log.Fatal(err)
			return
		}

		start := time.Now()
		var matched []index.Doc
		if source == "local" {
			searcher := singleton.NewSearcher(conf.Store.IndexFile)
			log.Printf("index loaded %d keys in %v", searcher.Segment.BT.Count(), time.Since(start))
			matched = searcher.Search(query)
		} else if source == "remote" {
			cli := cluster.NewSearchClient(&conf.Cluster.ManageServer)
			matched, err = cli.Search(query)
			if err != nil {
				log.Fatal(err)
				return
			}
		}
		log.Printf("Search found %d documents in %v", len(matched), time.Since(start))
		for _, d := range matched {
			doc := docs[d.ID]
			log.Printf("%d\t%s\n", d.ID, doc.Text)
		}
	} else if module == "merger" {
		singleton.Merge(srcPath, dstPath)
	} else if module == "cluster" {
		if host != "" && port != 0 {
			conf.Server.Host = host
			conf.Server.Port = port
		}
		if servername == "all" {
			log.Println("Starting Standalone Cluster..")
			if err := startStandaloneCluster(); err != nil {
				panic(err)
			}
		} else if servername == "managerserver" {
			log.Println("Starting ManagerServer..")
			svr := cluster.NewManagerServer(conf)
			svr.Run()
		} else if servername == "dataserver" {
			log.Println("Starting DataServer..")
			ds := cluster.NewDataServer(conf)
			ds.Run()
		} else if servername == "searchserver" {
			log.Println("Starting SearchServer..")
			srh := cluster.NewSearchServer(conf)
			srh.Run()
		}
	}
}
