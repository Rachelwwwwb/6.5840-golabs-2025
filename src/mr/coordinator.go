package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	nReduce      int
	progress     int            // 0 for map, 1 for reduce, 2 for done
	workerStatus map[int]bool   // worker id, idle or not
	taskMap      map[string]int // file name/reduce task id, status (0 not done, 1 in progress, 2 done);
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapReduceHandler(args *TaskArgs, reply *TaskReply) error {
	workId := args.WorkerId
	if _, exists := c.workerStatus[args.WorkerId]; !exists {
		workId = len(c.workerStatus)
		c.workerStatus[workId] = true
	}
	reply.WorkerId = workId

	// if the work is done, mark the work done
	if args.Status == 1 {
		c.taskMap[args.File] = 2
	}

	notDone := false
	// find the first available task
	for task := range c.taskMap {
		if c.taskMap[task] == 0 {
			reply.InputFile = task
			c.taskMap[task] = 1
			c.workerStatus[workId] = false
			if c.progress == 1 {
				var reduceId int
				if _, err := fmt.Sscanf(task, "reduce_%d", &reduceId); err == nil {
					reply.ReduceId = reduceId
				} else {
					log.Printf("Error parsing reduce ID from task name %s: %v", task, err)
				}
			}
			break
		}

		if c.taskMap[task] == 1 {
			notDone = true
		}
	}

	// status change;
	if reply.InputFile == "" && !notDone {
		switch c.progress {
		case 0:
			c.taskMap = make(map[string]int)
			for i := 0; i < c.nReduce; i++ {
				taskName := fmt.Sprintf("reduce_%d", i)
				c.taskMap[taskName] = 0
			}
			c.progress = 1
		case 1:
			reply.Done = true // no more task
			c.progress = 2
			return nil
		}
	}
	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.progress == 2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{nReduce, 0,
		make(map[int]bool),
		make(map[string]int)}

	for _, file := range files {
		c.taskMap[file] = 0
	}
	c.server()
	return &c
}
