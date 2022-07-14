package index

import (
	"compress/gzip"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// Document represents a Wikipedia abstract dump document.
type Document struct {
	Title string `xml:"title"`
	URL   string `xml:"url"`
	Text  string `xml:"abstract"`
	Timestamp int
	ID    int
}

// LoadDocuments loads a Wikipedia abstract dump and returns a slice of documents.
// Dump example from https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
func LoadDocuments(path string) ([]Document, error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abspath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()


	dump := struct {
		Documents []Document `xml:"doc"`
	}{}
	dec := xml.NewDecoder(gz)
	dec.Token()
	if err := dec.Decode(&dump); err != nil {
		return nil, err
	}
	docs := dump.Documents
	for i := range docs {
		docs[i].ID = i
	}
	return docs, nil
}

func LoadDocumentStream(path string) (chan *Document, error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abspath)
	if err != nil {
		return nil, err
	}

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	ch := make(chan *Document, 10)

	dec := xml.NewDecoder(gz)
	go func() {
		defer f.Close()
		defer gz.Close()
		id := 0
		for {
			tok, err := dec.Token()
			if tok == nil && err == io.EOF {
				ch <- nil
				// EOF means we're done.
				log.Println("EOF means we're done.")
				break
			} else if err != nil {
				//log.Fatalf("Error decoding token: %s", err.Error())
				panic(err)
			}

			switch ty := tok.(type) {
			case xml.StartElement:
				if ty.Name.Local == "doc" {
					// If this is a start element named "location", parse this element
					// fully.
					doc := Document{}
					if err = dec.DecodeElement(&doc, &ty); err != nil {
						//log.Fatalf("Error decoding item: %s", err.Error())
						panic(err)
					}
					id++
					doc.ID = id
					ch <- &doc
					if id % 1000 == 0 {
						fmt.Printf("load %d docs\n", id)
					}
				}
			default:
			}
		}
	}()
	return ch, nil
}