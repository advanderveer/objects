package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/apex/go-apex"
)

func main() {
	apex.HandleFunc(func(event json.RawMessage, ctx *apex.Context) (interface{}, error) {
		fmt.Fprintf(os.Stderr, "input %d bytes\n", len(event))

		return "hello", nil
	})
}
