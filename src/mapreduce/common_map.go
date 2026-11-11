package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string,       // the name of the MapReduce job
	mapTaskNumber int,    // which map task this is
	input_argument string, //the input argument for the map function
	inFile string,        // name of the input file
	nReduce int,          // the number of reduce tasks that will be run ("R" in the paper)
	mapF func(file string, arg string, contents string) []KeyValue,
) {

	// Read input
	data, err := os.ReadFile(inFile)
	checkError(err)

	kvs := mapF(inFile, input_argument, string(data))

	// Prepare encoder per reduce partition and keep files 
	type encFile struct{
		enc *json.Encoder
		f   *os.File
	}

	parts := make([]encFile, nReduce)

	for r := 0; r < nReduce; r++ {
		fname := reduceName(jobName, mapTaskNumber, r)
		f, err := os.Create(fname)
		checkError(err)
		parts[r] = encFile{enc: json.NewEncoder(f), f: f}
	}

	// kvs goes to partition via JSON
	for _, kv := range kvs {
		r := int(ihash(kv.Key) % uint32(nReduce))
		err := parts[r].enc.Encode(&kv)
		checkError(err)
	}

	// Close files
	for _, p := range parts {
		err := p.f.Close()
		checkError(err)
	}
	
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
