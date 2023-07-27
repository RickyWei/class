package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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
type Task struct {
	ID         int
	Status     int //0 unassigned 1 assign 2 finished
	FileNames  []string
	AssignTime time.Time
	NReduce    int
}

type AskMapTaskRequest struct {
}

type AskMapTaskResponse struct {
	Task *Task
}

type FinishMapTaskRequest struct {
	MapTaskID int
	FileNames []string
}

type FinishMapTaskResponse struct {
}

type AskReduceTaskRequest struct {
}

type AskReduceTaskResponse struct {
	Task *Task
}

type FinishReduceTaskRequest struct {
	ReduceTaskID int
}

type FinishReduceTaskResponse struct {
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
