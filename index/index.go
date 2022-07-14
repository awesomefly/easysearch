package index

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"sort"
)

type SearchModel int

const (
	Boolean SearchModel = iota
	VectorSpace
	BM25
)

type KVPair struct {
	Key   string
	Value PostingList
}

type Index interface {
	Property() *Property
	Keys() []string
	Clear()

	Add(docs []Document)
	Get(term string) []Doc

	Retrieval(must []string, should []string, not []string, k int, r int, m SearchModel) []Doc
}

// DoRetrieval returns top k docs sorted by boolean model
// todo: compress posting list and opt intersection/union rt
// https://blog.csdn.net/weixin_39890629/article/details/111268898
func DoRetrieval(idx Index, must []string, should []string, not []string, k int, r int, model SearchModel) []Doc {
	tfidf := NewTFIDF()

	//query's term frequency
	tfidf.DOC2TF[VirtualQueryDocId] = make(TF, 0)

	calTFIDF := func(term string, dn, df int, plr PostingList) {
		tfidf.IDF[term] = CalIDF(dn, df)
		for _, doc := range plr {
			var tf TF
			if tf = tfidf.DOC2TF[doc.ID]; tf == nil {
				tf = make(TF, 0)
			}
			tf[term] = doc.TF
			tfidf.DOC2TF[doc.ID] = tf
		}
	}
	properties := idx.Property()

	var result PostingList
	for _, term := range must {
		tfidf.DOC2TF[VirtualQueryDocId][term]++
		if pl := (PostingList)(idx.Get(term)); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))] //胜者表按TF排序,截断前r个,加速归并
			sort.Sort(plr)                                 //按docID排序
			if result == nil {
				result = plr
			} else {
				result.Inter(plr)
			}
			calTFIDF(term, properties.DocNum(), len(pl), plr)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range should {
		tfidf.DOC2TF[VirtualQueryDocId][term]++
		if pl := (PostingList)(idx.Get(term)); pl != nil {
			plr := pl[:IfElseInt(len(pl) > r, r, len(pl))]
			sort.Sort(plr)
			if result == nil {
				result = plr //胜者表，截断r
			} else {
				result.Union(plr)
			}
			calTFIDF(term, properties.DocNum(), len(pl), plr)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	for _, term := range not {
		if pl := (PostingList)(idx.Get(term)); pl != nil {
			sort.Sort(pl)
			result.Filter(pl)
		} else {
			// Token doesn't exist.
			continue
		}
	}

	if model == BM25 {
		result = CalBM25(result, tfidf, properties.TokenCount(), properties.DocNum())
	} else if model == VectorSpace {
		result = CalCosine(result, tfidf)
	}

	//排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score > result[j].Score //降序
	})
	log.Printf("result sorted:%+v", result)

	if len(result) > k {
		return result[:k]
	}
	return result
}

// Drain data to file. sort by key
func Drain(idx Index, file string) {
	if idx.Property().docNum == 0 {
		return
	}

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(err.Error())
	}

	keys := idx.Keys()
	sort.Strings(keys)

	buffer := bytes.NewBuffer([]byte{})
	for i := 0; i < len(keys); i++ {
		k := keys[i]
		pl := (PostingList)(idx.Get(k)).Bytes()

		buffer.Reset()
		l := int32(len(k))
		if err := binary.Write(buffer, binary.LittleEndian, l); err != nil {
			panic(err)
		}

		if err := binary.Write(buffer, binary.LittleEndian, []byte(k)); err != nil {
			panic(err)
		}

		l = int32(len(pl))
		if err := binary.Write(buffer, binary.LittleEndian, l); err != nil {
			panic(err)
		}

		if err := binary.Write(buffer, binary.LittleEndian, pl); err != nil {
			panic(err)
		}

		if _, err := fd.Write(buffer.Bytes()); err != nil {
			panic(err)
		}
	}
	fd.Close()
}

// Load file.
func Load(file string) (chan *KVPair, error) {
	ch := make(chan *KVPair, 10)

	fd, err := os.OpenFile(file, os.O_RDONLY|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}

	ReadInt := func() (int, error) {
		buf := make([]byte, 4)
		if n, err := fd.Read(buf); err != nil {
			return 0, err
		} else {
			var leng int32
			if err = binary.Read(bytes.NewBuffer(buf[:n]), binary.LittleEndian, &leng); err != nil {
				panic(err)
			}
			return int(leng), nil
		}

	}

	ReadString := func(n int) (string, error) {
		buf := make([]byte, n)
		if n, err := fd.Read(buf); err != nil {
			return "", err
		} else {
			return string(buf[:n]), nil
		}

	}

	go func() {
		defer fd.Close()

		for {
			pair := KVPair{}
			n, err := ReadInt()
			if err != nil {
				ch <- nil
				break
			}
			if pair.Key, err = ReadString(n); err != nil {
				ch <- nil
				break
			}
			if n, err = ReadInt(); err != nil {
				ch <- nil
				break
			}

			var v string
			if v, err = ReadString(n); err != nil {
				ch <- nil
				break
			}
			pair.Value.FromBytes([]byte(v))
			ch <- &pair
		}
	}()
	return ch, nil
}
