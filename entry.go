package main

import (
	"image"
	"io"
	"os"

	"github.com/andybalholm/dhash"
	"github.com/spaolacci/murmur3"
	filetype "gopkg.in/h2non/filetype.v1"

	_ "image/jpeg"
	_ "image/png"
)

type Entry struct {
	Path    string
	Hash    uint64
	IsImage bool
	DHash   dhash.Hash
}

func New(path string) (Entry, error) {
	e := Entry{path, 0, false, dhash.Hash{}}

	f, err := os.Open(path)
	if err != nil {
		return e, err
	}
	defer f.Close()

	hash := murmur3.New64()

	classified := false
	buf := make([]byte, 1024)
	for {
		n, err := f.Read(buf)

		if n > 0 {
			hash.Write(buf)
			if !classified {
				classified = true
				kind, unknown := filetype.Match(buf)
				if unknown == nil && (kind.Extension == "jpg" || kind.Extension == "png") {
					e.IsImage = true
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return e, err
		}
	}

	e.Hash = hash.Sum64()

	if classified && e.IsImage {
		f.Seek(0, 0)
		m, _, err := image.Decode(f)
		if err != nil {
			return e, err
		}
		e.DHash = dhash.New(m)
	}

	return e, nil
}
