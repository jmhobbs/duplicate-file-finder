package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/spaolacci/murmur3"
	"github.com/willf/bloom"
)

type Entry struct {
	Path string
	Hash uint64
}

func NewEntry(path string) (Entry, error) {
	e := Entry{path, 0}

	f, err := os.Open(path)
	if err != nil {
		return e, err
	}
	defer f.Close()

	hash := murmur3.New64()

	buf := make([]byte, 1024)
	for {
		n, err := f.Read(buf)

		if n > 0 {
			hash.Write(buf)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return e, err
		}
	}

	e.Hash = hash.Sum64()

	return e, nil
}

func Scan(parent_path string, wg *sync.WaitGroup, entries chan Entry) {
	defer wg.Done()

	paths, err := ioutil.ReadDir(parent_path)
	if err != nil {
		// TODO: Propagate these
		log.Printf("error: %s\n", err)
		return
	}

	for _, dirent := range paths {
		fullPath := path.Join(parent_path, dirent.Name())

		if dirent.IsDir() {
			wg.Add(1)
			go Scan(fullPath, wg, entries)
		} else {
			e, err := NewEntry(fullPath)
			if err != nil {
				log.Println("error:", err)
			}
			entries <- e
		}
	}
}

func main() {
	base_path := path.Dir(os.Args[1])
	fmt.Println("Searching for duplicates on path:", base_path, "\n")

	// TODO: Tweak these numbers
	filter := bloom.New(20000, 5)
	files := map[uint64]string{}
	collisions := map[string][]string{}

	var wg sync.WaitGroup
	entries := make(chan Entry)
	done := make(chan bool)

	go func() {
		hash_bytes := make([]byte, 8)

		for entry := range entries {
			binary.BigEndian.PutUint64(hash_bytes, entry.Hash)

			if filter.Test(hash_bytes) {
				collided_path := files[entry.Hash]
				collisions[collided_path] = append(collisions[collided_path], entry.Path)
			} else {
				filter.Add(hash_bytes)
				files[entry.Hash] = entry.Path
			}
		}

		close(done)
	}()

	// Walk the tree
	wg.Add(1)
	Scan(base_path, &wg, entries)
	wg.Wait()
	close(entries)

	<-done

	for root, cols := range collisions {
		fmt.Printf("%s\n", root)
		for _, c := range cols {
			fmt.Printf("  - %s\n", c)
		}
		fmt.Println("")
	}
}
