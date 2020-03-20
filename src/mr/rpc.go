package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int32

const (
	MAP_TASK    TaskType = 0
	REDUCE_TASK TaskType = 1
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Args struct{ X int }
type Reply struct{ Y int }
type ExampleArgs struct {
	X int
}
type GetTaskArgs struct {
	WorkerName string
}
type GetTaskReply struct {
	TaskType  TaskType
	FileNames []string
	TaskId    int
	NReduce   int
}
type NotifyArgs struct {
	TaskType TaskType
	TaskId   int
}
type NotifyReply struct {
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
