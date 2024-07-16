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

type TaskState int

const (
	WAITING TaskState = iota
	PROCESSING
	FINISHED
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapTask struct {
	Id       int
	FileName string
	NReduce  int
	State    TaskState
}

type ReduceTask struct {
	Id    int
	NMap  int
	State TaskState
}

type FetchTaskArgs struct{}
type FetchTaskReply struct {
	Done       bool
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskFinishArgs struct {
	TaskId int
}
type TaskFinishReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
