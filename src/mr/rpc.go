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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Cmd string

const (
	MAP    Cmd = "map"
	REDUCE Cmd = "reduce"
	WAIT   Cmd = "wait"
	EXIT   Cmd = "exit"
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	Command   Cmd
	NReduce   int
	NMap      int
	TaskId    int
	Filenames []string
}

type SetTaskDoneArgs struct {
	Command Cmd
	TaskId  int
}

type SetTaskDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
