package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type TaskArgs struct {
	WorkerId int
	Status   int // 0 idle, 1 work done
	File     string
}

type TaskReply struct {
	TaskID    int
	InputFile string
	MapId     int
	ReduceId  int
	WorkerId  int
	TaskType  int // 0 map, 1 reduce
	Done      bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
