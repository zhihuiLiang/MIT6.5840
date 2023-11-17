package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
)

const (
	RequestMap    = 1
	RequestReduce = 2
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum   int32
	MapTaskID   int32
	MapTaskChan chan string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	if args.TaskType == RequestMap {
		reply.Filename = <-c.MapTaskChan
		reply.WorkerID = c.MapTaskID
		reply.ReduceNum = c.ReduceNum
		atomic.AddInt32(&c.MapTaskID, 1)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.ReduceNum = int32(nReduce)
	c.MapTaskID = 1
	for _, filename := range files {
		c.MapTaskChan <- filename
	}

	c.server()
	return &c
}
