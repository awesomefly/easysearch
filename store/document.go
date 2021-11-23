package store

import (
	"compress/gzip"
	"encoding/xml"
	"os"
)

// Document represents a Wikipedia abstract dump document.
type Document struct {
	Title string `xml:"title"`
	URL   string `xml:"url"`
	Text  string `xml:"abstract"`
	ID    int
}

// LoadDocuments loads a Wikipedia abstract dump and returns a slice of documents.
// Dump example from https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract1.xml.gz
func LoadDocuments(path string) ([]Document, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	dec := xml.NewDecoder(gz)
	dump := struct {
		Documents []Document `xml:"doc"`
	}{}
	if err := dec.Decode(&dump); err != nil {
		return nil, err
	}
	docs := dump.Documents
	for i := range docs {
		docs[i].ID = i
	}
	return docs, nil
}
