package mapreduce

import (
	"encoding/json"
	"io"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// Gather values for the keys
	buckets := make(map[string][]string)

	for e := 0; e < nMap; e++ {
		fname := reduceName(jobName, e, reduceTaskNumber)
		f, err := os.Open(fname)
		checkError(err)

		deCode := json.NewDecoder(f)
		for {
			var kv KeyValue
			err := deCode.Decode(&kv)
			if err == io.EOF {
				break
			}
			checkError(err)
			buckets[kv.Key] = append(buckets[kv.Key], kv.Value)
		}
		checkError(f.Close())
	}

	// Sort keys
	keys := make([]string, 0, len(buckets))
	for e := range buckets {
		keys = append(keys, e)
	}
	sort.Strings(keys)

	// Write output as JSON
	oName := mergeName(jobName, reduceTaskNumber)
	out, err := os.Create(oName)
	checkError(err)

	enCode := json.NewEncoder(out)
	for _, k := range keys {
		res := reduceF(k, buckets[k])
		checkError(enCode.Encode(&KeyValue{Key: k, Value: res}))
	}

	checkError(out.Close())
}
