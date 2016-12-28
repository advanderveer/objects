package main_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
)

func liveAPIEndpoint(t *testing.T) string {
	buf := bytes.NewBuffer(nil)
	cmd := exec.Command("terraform", "output", "api_endpoint")
	cmd.Stderr = os.Stderr
	cmd.Stdout = buf
	err := cmd.Run()
	if err != nil {
		t.Skip("skipping integration, couldnt get api endpoint through terraform: " + err.Error())
	}

	return strings.TrimSpace(buf.String())
}

//concurrent upload
func upload(cr *chunker.Chunker, concurrency int) (err error) {
	type result struct {
		err error
	}

	type item struct {
		chunk []byte
		resCh chan *result
		err   error
	}

	work := func(it *item) {
		start := time.Now()
		resp, err := http.Post("https://jau6ikmzt7.execute-api.eu-west-1.amazonaws.com/test", "application/octet-stream", bytes.NewBuffer(it.chunk))
		if err != nil {
			it.resCh <- &result{fmt.Errorf("failed to execute request: %v", err)}
			return
		}

		if resp.StatusCode != http.StatusOK {
			it.resCh <- &result{fmt.Errorf("unexpected status code: %v", resp.Status)}
			return
		}

		log.Println(resp.Status, len(it.chunk)/1024, "KiB", time.Since(start))
		it.resCh <- &result{}
	}

	//fan out
	itemCh := make(chan *item, concurrency)
	go func() {
		defer close(itemCh)
		buf := make([]byte, chunker.MaxSize)
		for {
			chunk, err := cr.Next(buf)
			if err != nil {
				if err != io.EOF {
					itemCh <- &item{err: err}
				}

				break
			}

			it := &item{
				chunk: chunk.Data,
				resCh: make(chan *result),
			}

			go work(it)  //create work
			itemCh <- it //send to fan-in thread for syncing results
		}
	}()

	//fan-in
	for it := range itemCh {
		if it.err != nil {
			return fmt.Errorf("failed to iterate: %v", it.err)
		}

		res := <-it.resCh
		if res.err != nil {
			return res.err
		}
	}

	return nil
}

func randr(size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), size)
}

func randb(size int64) []byte {
	b, err := ioutil.ReadAll(randr(size))
	if err != nil {
		panic(err)
	}

	return b
}

func BenchmarkUpload(b *testing.B) {
	size := int64(12 * 1024 * 1024)
	data := randb(size)

	//the following cant be larger then 6MiB: {"b":"$input.body"}
	log.Println("max λ:", int64(base64.URLEncoding.DecodedLen(6*1024*1024)-9), "max chunker:", chunker.MaxSize)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		cr := chunker.New(r, chunker.Pol(0x3DA3358B4DC173))
		err := upload(cr, 64)
		if err != nil {
			b.Error(err)
		}
	}
}
