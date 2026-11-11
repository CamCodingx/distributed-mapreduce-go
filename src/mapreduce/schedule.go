package mapreduce

import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup

	for taskNum := 0; taskNum < ntasks; taskNum++ {
		wg.Add(1)

		go func(task int) {
			defer wg.Done()

			// Build task args
			args := DoTaskArgs{
				JobName:      mr.jobName,
				InputArg:     mr.arg,
				Phase:        phase,
				TaskNumber:   task,
				NumOtherPhase: nios,
			}

			if phase == mapPhase {
				args.File = mr.files[task]
			}

			for {
				// Get worker (exists or new registration)
				worker := <-mr.registerChannel

				ok := call(worker, "Worker.DoTask", &args, new(struct{}))
				if ok {

					// Give worker back to reuse
					go func(w string) {
						mr.registerChannel <- w
					}(worker)
					break
				}

				// Worker failed (do not place back try again with new one) (fail check)
				debug("Schedule: worker %s failed for task %d (%v); retrying\n",
					worker, task, phase)
			}
		}(taskNum)
	}

	// Wait for all tasks in this phase
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
