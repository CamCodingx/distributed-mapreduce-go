# Distributed MapReduce Framework in Go

A fault-tolerant, concurrent distributed MapReduce processing framework implemented in Go, modeled after Google's foundational MapReduce paper (Dean & Ghemawat). 

This framework coordinates master and worker nodes over Remote Procedure Calls (RPC), manages intermediate partitioned storage, and executes parallel Map and Reduce pipelines with automated worker failure recovery.

---

## Architecture Overview

```
                      +-------------------+
                      |    Master Node    |
                      | (Task Scheduler)  |
                      +---------+---------+
                                |
             +------------------+------------------+
             | RPC (Worker Pool via Channels)      |
             v                                     v
    +-----------------+                   +-----------------+
    |  Worker Node 1  |                   |  Worker Node 2  |
    +--------+--------+                   +--------+--------+
             |                                     |
   [Map: Read Input Chunk]               [Map: Read Input Chunk]
             |                                     |
    Partition via Hash:                   Partition via Hash:
   ihash(key) % nReduce                  ihash(key) % nReduce
             |                                     |
             +------------------+------------------+
                                |
                   Intermediate JSON Partitions
                     (mrtmp.<job>-<map>-<r>)
                                |
             +------------------+------------------+
             v                                     v
    +-----------------+                   +-----------------+
    | Reduce Worker 1 |                   | Reduce Worker 2 |
    +--------+--------+                   +--------+--------+
             |                                     |
   [Sort Keys Lexically]                 [Sort Keys Lexically]
   [Aggregate Values]                    [Aggregate Values]
             |                                     |
             v                                     v
    Final Output File 0                   Final Output File 1
                     \                     /
                      v                   v
                    [Master Merges Output Files]
```

### Execution Lifecycle

1. **Job Initialization:**
   The master process is initialized with a list of input files, the target job name, the number of reduce tasks (`nReduce`), and user-defined `Map` and `Reduce` functions.
2. **RPC Worker Registration:**
   Workers register with the master via UNIX domain socket RPC (`Master.Register`). The master pushes available worker addresses onto a concurrent thread-safe registration channel (`registerChannel`).
3. **Map Phase:**
   - The master divides input files into $M$ map tasks.
   - Workers execute `doMap()`, which calls the application's `Map` function on the file contents.
   - Output key-value pairs are partitioned into $R$ intermediate JSON files using an FNV-1a hash function: `r = ihash(key) % nReduce`.
4. **Reduce Phase:**
   - Once all map tasks complete successfully, the master schedules $R$ reduce tasks.
   - For each reduce partition $r$, the assigned worker gathers intermediate files from all $M$ map tasks (`mrtmp.<job>-<m>-<r>`).
   - The worker sorts keys lexicographically, aggregates all values for identical keys, and executes the user's `Reduce` function.
   - Results are serialized as JSON records.
5. **Merge & Verification:**
   The master aggregates all reduce outputs into a single consolidated result file and terminates worker processes cleanly.

---

## Key Technical Highlights & Engineering Challenges

### 1. Concurrent Worker Scheduling (`schedule.go`)
- Utilizes Go goroutines and `sync.WaitGroup` to dispatch and synchronize parallel task execution.
- Implements a non-blocking worker pool using Go channels (`registerChannel`). Available workers are pulled off the channel, assigned work, and returned upon task completion.

### 2. Fault Tolerance & Automatic Retry Handling
- In distributed environments, worker nodes can crash, stall, or experience network partitions.
- If an RPC call `call(worker, "Worker.DoTask", ...)` returns `false` (indicating a failed or unreachable worker), the master:
  1. Drops the dead worker from the active pool.
  2. Re-queues the failed task to be assigned to the next healthy worker from `registerChannel`.
  3. Ensures idempotent execution so that failed partial runs do not corrupt final output.

### 3. Intermediate Data Partitioning (`common_map.go`)
- Implements hash-based deterministic partitioning (`ihash(key) % nReduce`) to guarantee all values for a given key route to the exact same reduce worker.
- Encodes intermediate key-value streams into JSON buffers to ensure cross-process serialization safety and persistent storage.

### 4. Deterministic Aggregation & Sorting (`common_reduce.go`)
- Buffers partitioned inputs into in-memory key-to-value-list maps (`map[string][]string`).
- Applies in-place lexicographical sorting on all extracted keys before invoking the `reduceF` callback, producing strictly reproducible, ordered results.

---

## Repository Structure

```
distributed-mapreduce-go/
├── .gitignore                      # Ignores binaries and temporary test artifacts
├── README.md                       # Architectural documentation and guide
└── src/
    ├── main/
    │   ├── dgrep.go                # Distributed Grep application
    │   ├── dgrepcount.go           # Distributed Pattern Frequency Counter
    │   ├── test-mr.sh              # Full end-to-end integration test runner
    │   ├── test-dgrep.sh           # Test script for distributed grep
    │   └── test-dgrepcount.sh      # Test script for distributed word counting
    └── mapreduce/
        ├── common.go               # Shared types, file naming conventions, debug logging
        ├── common_map.go           # Map worker implementation & hash partitioning
        ├── common_reduce.go        # Reduce worker implementation & key aggregation
        ├── common_rpc.go           # RPC argument definitions & client call helpers
        ├── master.go               # Master coordinator lifecycle & execution loop
        ├── master_rpc.go           # Master RPC listener & worker registration handler
        ├── master_splitmerge.go    # File splitting and intermediate output merger
        ├── schedule.go             # Parallel task dispatcher with fault tolerance
        ├── worker.go               # Worker RPC server loop & task executor
        ├── test_test.go            # Comprehensive unit & failure recovery test suite
        └── readme.go               # Package documentation
```

---

## Applications Included

### 1. Distributed Grep (`src/main/dgrep.go`)
A distributed regex pattern matcher that searches multiple input files concurrently across worker nodes, emitting matching lines and file offsets.

### 2. Distributed Match Counter (`src/main/dgrepcount.go`)
Counts the total occurrences of specified patterns across large distributed text corpora, combining map-side counts with reduce-side aggregations.

---

## Running Tests & Verifying Output

### Prerequisites
- Go 1.18 or later
- Linux, macOS, or WSL (Windows Subsystem for Linux) for UNIX domain socket RPC support

### Setup Environment
Set your `GOPATH` to the project root:

```bash
cd distributed-mapreduce-go
export GOPATH=$(pwd)
```

### Run the Full Test Suite
Execute the automated test script:

```bash
cd src/main
bash test-mr.sh
```

### Test Suite Coverage
The test suite (`src/mapreduce/test_test.go`) validates:
- **`TestSequentialSingle`**: Validates basic map and reduce correctness on a single input file in sequential mode.
- **`TestSequentialMany`**: Verifies multi-file input handling and intermediate partition distribution.
- **`TestBasic`**: Spawns multiple concurrent workers over RPC and validates parallel execution speed and output parity.
- **`TestOneFailure`**: Simulates a worker crash during the Map phase, verifying the master successfully reschedules the failed task.
- **`TestManyFailures`**: Simulates multiple concurrent worker crashes across both Map and Reduce phases, validating robust error recovery without data loss.

---

## Technical Interview Discussion Points

When discussing this project in software engineering interviews, key topics to highlight:
- **Why UNIX domain sockets for RPC?** High throughput and low latency on local simulated clusters without port collision or TCP overhead.
- **Handling Worker Failures:** The master uses Go channels to decouple task generation from worker allocation. A worker is never returned to `registerChannel` if its RPC call fails, preventing poison-pill workers from repeatedly failing tasks.
- **Concurrency Control:** How `sync.WaitGroup` coordinates phase transitions—the Reduce phase strictly waits for all Map tasks to finish to prevent reading incomplete intermediate partition files.
