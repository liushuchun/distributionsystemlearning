package mapreduce

import "fmt"
import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var wg = &sync.WaitGroup{}
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce

	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, taskNum int, nios int, phase jobPhase) {
			var wk string
			defer wg.Done()
			for {

				wk = <-mr.registerChannel
				args := &DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[taskNum],
					Phase:         phase,
					TaskNumber:    taskNum,
					NumOtherPhase: nios,
				}
				ok := call(wk, "Worker.DoTask", args, new(struct{}))
				if ok == false {
					fmt.Printf("Master:Scheduler failed:%v\n", args)
				} else {
					go func() {
						mr.registerChannel <- wk
					}()
					break
				}
			}
		}(wg, i, nios, phase)

	}
	wg.Wait() //这个不要忘记
	fmt.Printf("Schedule: %v phase done\n", phase)
}
