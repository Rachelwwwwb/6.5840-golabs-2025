package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := &TaskArgs{}
	for {
		if PullTask(args, mapf()) {
			break
		}
	}

}

func PullTask(args *TaskArgs, mapf func(string, string) []KeyValue) bool {
	reply := TaskReply{}

	ok := call("Coordinator.MapReduceHandler", &args, &reply)
	if ok {
		if reply.Done {
			return true
		}
		args.WorkerId = reply.WorkerId

		switch reply.TaskType {
		case 0:
			handleMapTask(&reply, mapf)
		case 1:
			handleReduceTask(&reply)
		}
		args.Status = 1
		args.File = reply.InputFile
	} else {
		fmt.Printf("call failed!\n")
	}
	return false
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func handleMapTask(reply *TaskReply, mapf func(string, string) []KeyValue) {
	// read from the file
	intermediate := []KeyValue{}
	filename := reply.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// sorting
	sort.Sort(ByKey(intermediate))

	oname := "mr-" + reply.MapId + "-" + reply.ReduceId
	ofile, _ := os.Create(oname)

}

func handleReduceTask(reply *TaskReply) {

}
