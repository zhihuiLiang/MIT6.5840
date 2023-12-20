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

// Add your RPC definitions here.
type MapTaskRequestArgs struct {
}

type MapTaskRequestReply struct {
	HasMapTask bool
	Filename   string
	MapTaskID  int32
	ReduceNum  int32
}

type MapDoneTaskRequestArgs struct {
}

type MapDoneTaskRequestReply struct {
}

type ReduceTaskRequestArgs struct {
}

type ReduceTaskRequestReply struct {
	ReduceTaskID int32
	MaxMapTaskID int32
}

type GoReduceRequestArgs struct {
}

type GoReduceRequestReply struct {
	Ok bool
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
