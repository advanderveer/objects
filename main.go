//go:generate rotorgen build.zip
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/nerdalize/rotor/rotor"
	"github.com/smartystreets/go-aws-auth"
)

//Has attempts to download header info for an S3 k
func Has(client *http.Client, k []byte, host, bucket string, creds awsauth.Credentials) (has bool, err error) {
	raw := fmt.Sprintf("https://%s/%s/%x", host, bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return false, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("HEAD", loc.String(), nil)
	if err != nil {
		return false, fmt.Errorf("failed to create HEAD request: %v", err)
	}

	awsauth.Sign(req, creds)
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to perform HEAD request: %v", err)
	}

	if resp.StatusCode == http.StatusOK {
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		//AWS returns forbidden for a HEAD request if the one performing the operation does not have
		//list bucket permissions
		return false, nil
	} else {
		return false, fmt.Errorf("unexpected response from HEAD '%s' request: %s", loc, resp.Status)
	}
}

//Get attempts to download chunk 'k' from an S3 object store
func Get(client *http.Client, k []byte, host, bucket string, creds awsauth.Credentials) (resp *http.Response, err error) {
	raw := fmt.Sprintf("https://%s/%s/%x", host, bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("GET", loc.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GET request: %v", err)
	}

	awsauth.Sign(req, creds)
	resp, err = client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform GET request: %v", err)
	}

	return resp, nil
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func Put(client *http.Client, k []byte, body io.Reader, host, bucket string, creds awsauth.Credentials) error {
	raw := fmt.Sprintf("https://%s/%s/%x", host, bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), body)
	if err != nil {
		return fmt.Errorf("failed to create PUT request: %v", err)
	}

	awsauth.Sign(req, creds)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to perform PUT request: %v", err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body for unexpected response: %s", resp.Status)
		}

		return fmt.Errorf("unexpected response from PUT '%s' response: %s, body: %v", loc, resp.Status, string(body))
	}

	return nil
}

//Handler is our simple http handler that can be tested in unit tests
type Handler struct {
	Host         string
	Bucket       string
	MaxChunkSize int64
	MinChunkSize int64
	Client       *http.Client
	Creds        awsauth.Credentials
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL.Path)

	switch r.Method {
	//Get request are always attempts to fetch a certain
	//chunk from the s3 store. Since s3 supports read-after-write
	//consistency and put is append-only in our code we can always
	//expect that a 404 from the store means the chunk is not present
	case http.MethodGet:

		//check path requirements for decoding
		path := bytes.Trim([]byte(r.URL.Path), "/")
		if len(path) != base64.URLEncoding.EncodedLen(sha256.Size) {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		//attempt to decode, on failure report 404
		k := make([]byte, base64.URLEncoding.DecodedLen(len(path)))
		n, err := base64.URLEncoding.Decode(k, path)
		if err != nil || n != sha256.Size {
			log.Printf("failed to decode '%x' (%s): (%d/%d) %v", path, r.URL.Path, n, sha256.Size, err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		//pass on to s3
		s3resp, err := Get(h.Client, k, h.Host, h.Bucket, h.Creds)
		if err != nil {
			log.Printf("failed to get chunk with key '%s': %v", r.URL.Path, err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		//anything other then OK is reported as 404
		if s3resp.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		//copy over s3 response
		defer s3resp.Body.Close()
		_, err = io.Copy(w, s3resp.Body)
		if err != nil {
			log.Printf("failed to copy s3 response to api response: %v", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		//Put request are not passed on directly, since we're having an append-only
		//party with our content-based chunks we buffer the request body up to a
		//certain size and hash it ourselves to determine the storage key.
	case http.MethodPost:
		hash := sha256.New()
		buf := bytes.NewBuffer(nil)
		mw := io.MultiWriter(buf, hash)

		maxrc := http.MaxBytesReader(w, r.Body, h.MaxChunkSize)
		defer maxrc.Close()

		log.Println("copying", r.Header)
		written, err := io.Copy(mw, maxrc)
		if err != nil || written != int64(buf.Len()) {
			log.Printf("failed to copy request body into memory: (%d/%d) %v", written, buf.Len(), err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Println("copied", r.Header)

		//the limit reader went passed one byte passed the max size
		//report as such to the api user
		if written > int64(h.MaxChunkSize) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		//if the file is to too small we dont accept it, it may be an attempt to
		//find cheap hash collisions
		if written < int64(h.MinChunkSize) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		//sha2 key (32 bytes)
		k := hash.Sum(nil)

		//check if the key is already stored
		exists, err := Has(h.Client, k, h.Host, h.Bucket, h.Creds)
		if err != nil {
			log.Printf("failed to ask s3 for chunk existence: %v", err)
			w.WriteHeader(http.StatusInternalServerError) //indicate to the user he cannot proceed as expected
			return
		} else if exists {
			//chunk already exist, OK and encode key
			fmt.Fprintln(w, base64.URLEncoding.EncodeToString(k))
			return
		}

		//finally actually put the chunk
		err = Put(h.Client, k, buf, h.Host, h.Bucket, h.Creds)
		if err != nil {
			log.Printf("failed to put chunk from memory onto s3: %v", err)
			w.WriteHeader(http.StatusInternalServerError) //indicate to the user he cannot proceed as expected
			return
		}

		//chunk already exist, OK and encode key
		fmt.Fprintln(w, base64.URLEncoding.EncodeToString(k))
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func main() {
	h := &Handler{
		Client:       http.DefaultClient,
		Host:         fmt.Sprintf("s3-%s.amazonaws.com", os.Getenv("AWS_REGION")),
		Bucket:       os.Getenv("S3_BUCKET"),
		MinChunkSize: 1 * 1024 * 1024, //1MiB
		MaxChunkSize: 8 * 1024 * 1024, //8MiB
		Creds: awsauth.Credentials{
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SecurityToken:   os.Getenv("AWS_SESSION_TOKEN"),
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/objects/", h)

	log.Printf("%+v", h)

	log.Fatal(rotor.ServeHTTP(os.Stdin, os.Stdout, mux))
}
