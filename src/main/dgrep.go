package main

import (
	"fmt"
	"lab3/mapreduce"
	"os"
	"regexp"
	"strings"
)

// The mapping function is called once for each piece of the input.
// key:    encoded as "filename:lineNumber"
// value:  the text of the matching line (no newline)
func mapF(filename string, input_arg string, contents string) (res []mapreduce.KeyValue) {
	re, err := regexp.Compile(input_arg)
	if err != nil {
		// If the pattern is invalid, just return no matches.
		return res
	}

	lines := strings.Split(contents, "\n")
	for i, line := range lines {
		if re.MatchString(line) {
			key := fmt.Sprintf("%s:%d", filename, i+1)
			res = append(res, mapreduce.KeyValue{
				Key:   key,
				Value: line,
			})
		}
	}
	return res
}

// reduceF is called once per key (which is "filename:lineno").
// There should be exactly one value (the line text), so just return it.
func reduceF(key string, values []string) string {
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run dgrep.go master sequential pattern x1.txt .. xN.txt)
// 2) Master (e.g., go run dgrep.go master localhost:7777 pattern x1.txt .. xN.txt)
// 3) Worker (e.g., go run dgrep.go worker localhost:7777 localhost:7778 &)

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if len(os.Args) > 5 {
		if os.Args[1] == "master" {
			var mr *mapreduce.Master
			if os.Args[2] == "sequential" {
				mr = mapreduce.Sequential("dgrep", os.Args[3], os.Args[4:], 2, mapF, reduceF)
			} else {
				mr = mapreduce.Distributed("dgrep", os.Args[3], os.Args[4:], 2, os.Args[2])
			}
			mr.Wait()
		}
	} else if len(os.Args) == 4 && os.Args[1] == "worker" {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	} else {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	}
}
