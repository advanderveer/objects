//go:generate rotorgen build.zip
package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

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
	} else if resp.StatusCode == http.StatusNotFound {
		return false, nil
	} else {
		return false, fmt.Errorf("unexpected response from PUT '%s' request: %s", loc, resp.Status)
	}
}

//Get attempts to download chunk 'k' from an S3 object store
func Get(client *http.Client, k []byte, host, bucket string, creds awsauth.Credentials) (chunk []byte, err error) {
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
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform GET request: %v", err)
	}

	defer resp.Body.Close()
	chunk, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body for %s: %v", resp.Status, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected response from PUT '%s' request: %s, body: %v", loc, resp.Status, string(chunk))
	}

	return chunk, nil
}

//Put uploads a chunk to an S3 object store under the provided key 'k'
func Put(client *http.Client, k []byte, chunk []byte, host, bucket string, creds awsauth.Credentials) error {
	raw := fmt.Sprintf("https://%s/%s/%x", host, bucket, k)
	loc, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' as url: %v", raw, err)
	}

	req, err := http.NewRequest("PUT", loc.String(), bytes.NewBuffer(chunk))
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

		return fmt.Errorf("unexpected response from PUT '%s' request: %s, body: %v", loc, resp.Status, string(body))
	}

	return nil
}

var (
	region       = os.Getenv("AWS_REGION")
	bucket       = os.Getenv("S3_BUCKET")
	accessKey    = os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey    = os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken = os.Getenv("AWS_SESSION_TOKEN")
)

var handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	var (
		host = fmt.Sprintf("s3-%s.amazonaws.com", region)
		has  bool
		err  error
		data []byte
	)

	start := time.Now()
	if has, err = Has(
		http.DefaultClient,
		[]byte("my-key"),
		host,
		bucket,
		awsauth.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SecurityToken:   sessionToken,
		}); err != nil {
		fmt.Fprintf(w, "failed to has: %v", err)
	}

	fmt.Fprintf(w, "has duration: %s, has: %v\n", time.Since(start), has)

	start = time.Now()
	if data, err = Get(
		http.DefaultClient,
		[]byte("my-key"),
		host,
		bucket,
		awsauth.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SecurityToken:   sessionToken,
		}); err != nil {
		fmt.Fprintf(w, "failed to get: %v", err)
	}

	fmt.Fprintf(w, "get duration: %s, data: %x\n", time.Since(start), data)

	start = time.Now()
	if err = Put(
		http.DefaultClient,
		[]byte("my-key"),
		[]byte("my-data"),
		host,
		bucket,
		awsauth.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			SecurityToken:   sessionToken,
		}); err != nil {
		fmt.Fprintf(w, "failed to put: %v", err)
	}

	fmt.Fprintf(w, "put duration: %s\n", time.Since(start))
})

func main() {
	log.Fatal(rotor.ServeHTTP(os.Stdin, os.Stdout, handler))
}
