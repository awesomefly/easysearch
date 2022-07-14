package search

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/awesomefly/easysearch/config"

	"github.com/awesomefly/easysearch/index"
)

type Indexer interface {
	// Drain data to file. sort by key
	Drain(file string)
	Merge(file string)
}

const SpiltThresholdDocNum int = 100000
func Index(c config.Config) {
	log.Println("Starting index...")

	start := time.Now()
	//1. spilt to small file.
	ch, err := index.LoadDocumentStream(c.Store.DumpFile)
	if err != nil {
		log.Fatal(err)
		return
	}
	//remove old index files
	IndexDir := filepath.Dir(c.Store.IndexFile)
	IndexPathPrefix := "_tmp." + filepath.Base(c.Store.IndexFile)
	reg,_ := regexp.Compile(IndexPathPrefix + ".*")
	if err = Remove(IndexDir, reg); err != nil {
		log.Fatal(err)
		return
	}

	reg1, _ := regexp.Compile("^" + filepath.Base(c.Store.IndexFile) + ".*")
	if err = Remove(filepath.Dir(c.Store.IndexFile), reg1); err != nil {
		log.Fatal(err)
		return
	}

	//2. index and dump posting list
	idx := index.NewHashMapIndex()
	for {
		//timeout := time.NewTimer(1 * time.Second)
		select {
		case doc := <-ch:
			if doc == nil {
				fmt.Println("All doc have been read.")
				index.Drain(idx, fmt.Sprintf("%s.%d", IndexDir+"/"+IndexPathPrefix, time.Now().Nanosecond()))
				break
			}
			idx.Add([]index.Document{*doc}) //内存中操作
			if idx.Property().DocNum() >= SpiltThresholdDocNum {
				index.Drain(idx, fmt.Sprintf("%s.%d", IndexDir+"/"+IndexPathPrefix, time.Now().Nanosecond()))
				idx.Clear()
			}
			continue
		//case <-timeout.C:
		//	log.Printf("Read timeout. err: %s", err.Error())
		//	break
		}
		break
	}
	log.Printf("Dump all documents in %v.", time.Since(start))

	var chs []chan *index.KVPair
	files, err := Walk(IndexDir, reg)
	for i := 0; i < len(files); i++ {
		chl, err := index.Load(files[i])
		if err != nil {
			panic(err)
		}
		chs = append(chs, chl)
	}

	start = time.Now()
	bt := index.NewBTreeIndex(c.Store.IndexFile)

	//3. merge posting list
	//频繁往Posting List中追加doc，导致元分配空间不足，需要拷贝PostingList到新的空间，文件读写IO高
	//必须归并后在写入索引，
	finished := make(map[int]bool)
	pairs := make([]*index.KVPair, len(files))
	for {
		pivot := -1
		for i := 0; i < len(pairs); i++ {
			if pairs[i] == nil && chs[i] != nil {
				timeout := time.NewTimer(1000 * time.Millisecond)
				select {
				case kv := <-chs[i]:
					if kv == nil {
						close(chs[i])
						chs[i] = nil
					}
					pairs[i] = kv
				case <-timeout.C:
					close(chs[i])
					chs[i] = nil
				}
			}

			if pairs[i] == nil {//已完成一路
				finished[i] = true
				continue
			} else if pivot == -1 {//取第一非空值作为哨兵
				pivot = i
				continue
			}

			if pairs[i].Key < pairs[pivot].Key {
				pivot = i
			} else if pairs[i].Key == pairs[pivot].Key {
				pairs[pivot].Value.Append(pairs[i].Value...)
				pairs[i] = nil
			}
		}
		if len(finished) == len(files) { //all finished
			break
		}

		//4. insert "word->posting list"
		bt.Insert(pairs[pivot].Key, pairs[pivot].Value)
		pairs[pivot] = nil
	}
	log.Printf("Indexed %d documents and %d keys in %v", bt.Property().DocNum(), bt.BT.Count(), time.Since(start))

	bt.BT.Stats(true)
	bt.Close()
	time.Sleep(5*time.Second)
}

func Remove(dir string, reg *regexp.Regexp) error {
	files, err := Walk(dir, reg)
	if err != nil {
		return err
	}
	fmt.Printf("remove %d files.\n", len(files))
	for i := 0; i < len(files); i++ {
		//fmt.Println(files[i])
		os.Remove(files[i])
	}
	return nil
}

func Walk(dir string, re *regexp.Regexp) ([]string, error) {
	// Just a demo, this is how we capture the files that match
	// the pattern.
	var files []string

	walk := func(path string, d fs.DirEntry, err error) error {
		if re.MatchString(d.Name()) == false {
			return nil
		}
		if d.IsDir() {
			fmt.Println(path + string(os.PathSeparator))
		} else {
			//fmt.Println(path)
			files = append(files, path)
		}
		return nil
	}
	filepath.WalkDir(dir, walk)
	return files, nil
}

