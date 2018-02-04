package main

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/andybalholm/dhash"
	"github.com/willf/bloom"
)

type ScanError struct {
	Path string
	Err  error
}

func (err ScanError) Error() string {
	return fmt.Sprintf("%s: %v", err.Path, err.Err)
}

func Scan(parent_path string, wg *sync.WaitGroup, entries chan Entry, errors chan error) {
	defer wg.Done()

	paths, err := ioutil.ReadDir(parent_path)
	if err != nil {
		errors <- ScanError{parent_path, err}
		return
	}

	for _, dirent := range paths {
		fullPath := path.Join(parent_path, dirent.Name())

		if dirent.IsDir() {
			wg.Add(1)
			go Scan(fullPath, wg, entries, errors)
		} else {
			e, err := New(fullPath)
			if err != nil {
				errors <- ScanError{fullPath, err}
			}
			entries <- e
		}
	}
}

func main() {
	base_path := path.Dir(os.Args[1])
	log.Println("Searching for duplicates on path:", base_path)

	// TODO: Tweak these numbers
	filter := bloom.New(20000, 5)
	files := map[uint64]string{}
	collisions := map[string][]string{}
	images := map[dhash.Hash]string{}
	imageCollisions := map[dhash.Hash][]string{}

	var wg sync.WaitGroup
	entries := make(chan Entry)
	errors := make(chan error)
	done := make(chan bool)
	errDone := make(chan bool)

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

				if entry.IsImage {
					_, ok := images[entry.DHash]
					if ok {
						imageCollisions[entry.DHash] = append(imageCollisions[entry.DHash], entry.Path)
					} else {
						matched := false
						for h, _ := range images {
							if dhash.Distance(h, entry.DHash) < 10 {
								// Near DHash match
								imageCollisions[entry.DHash] = append(imageCollisions[entry.DHash], entry.Path)
								matched = true
								break
							}
						}

						if !matched {
							images[entry.DHash] = entry.Path
						}
					}
				}
			}
		}

		close(done)
	}()

	go func() {
		for err := range errors {
			log.Println(err)
		}

		close(errDone)
	}()

	// Walk the tree
	wg.Add(1)
	Scan(base_path, &wg, entries, errors)
	wg.Wait()
	close(entries)
	close(errors)

	<-done
	<-errDone

	log.Println("Done!", "\n")

	fmt.Println("== Exact Matches\n")
	for root, cols := range collisions {
		fmt.Printf("%s\n", root)
		for _, c := range cols {
			fmt.Printf("  - %s\n", c)
		}
		fmt.Println("")
	}

	fmt.Println("== Image Visual Matches\n")
	for root, cols := range imageCollisions {
		fmt.Printf("%s\n", images[root])
		for _, c := range cols {
			fmt.Printf("  - %s\n", c)
		}
		fmt.Println("")
	}
}
