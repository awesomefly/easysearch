package index

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func TestLoadDocumentStream(t *testing.T) {
	ch, err := LoadDocumentStream("../data/tem.xml")
	if err != nil {
		log.Fatal(err)
		return
	}

	for {
		//timeout := time.NewTimer(1 * time.Second)
		select {
		case doc := <-ch:
			if doc == nil {
				fmt.Println("doc is nil")
				return
			}
			fmt.Println(doc)
			//fmt.Printf("recv doc: %v", *doc)

			continue
			//case <-timeout.C:
			//	log.Printf("Read timeout. err: %s", err.Error())
			//	break
		}
		break
	}
	time.Sleep(5*time.Second)
}
