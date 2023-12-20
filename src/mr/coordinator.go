package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
)

const (
	RequestMap    = 1
	RequestReduce = 2
)

type Coordinator struct {
	// Your definitions here.
	ReduceNum        int32
	lock             sync.Mutex
	MapTaskID        int32
	ReduceTaskID     int32
	FinishMapTaskNum int32
	MaxMapTaskID     int32
	MapTaskChan      chan string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MapTaskRequest(args *MapTaskRequestArgs, reply *MapTaskRequestReply) error {
	filename, ok := <-c.MapTaskChan
	if ok {
		reply.HasMapTask = true
		reply.Filename = filename
		c.lock.Lock()
		reply.MapTaskID = c.MapTaskID
		c.MapTaskID += 1
		c.lock.Unlock()
		reply.ReduceNum = c.ReduceNum
	} else {
		reply.HasMapTask = false
	}

	return nil
}

func (c *Coordinator) MapDoneRequest(args *MapDoneTaskRequestArgs, reply *MapDoneTaskRequestReply) error {
	atomic.AddInt32(&c.FinishMapTaskNum, 1)
	return nil
}

func (c *Coordinator) GoReduceRequest(args *GoReduceRequestArgs, reply *GoReduceRequestReply) error {
	if atomic.LoadInt32(&c.FinishMapTaskNum) == c.MaxMapTaskID {
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (c *Coordinator) ReduceTaskRequest(arg *ReduceTaskRequestArgs, reply *ReduceTaskRequestReply) error {
	c.lock.Lock()
	id := c.ReduceTaskID
	if id >= c.ReduceNum {
		return nil
	}
	reply.ReduceTaskID = id
	c.ReduceTaskID += 1
	c.lock.Unlock()
	reply.MaxMapTaskID = c.MapTaskID
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
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
	c.MapTaskID = 0
	c.MapTaskChan = make(chan string, 100)
	for _, filename := range files {
		c.MapTaskChan <- filename
	}
	close(c.MapTaskChan)
	c.ReduceTaskID = 0
	c.MaxMapTaskID = int32(len(files))
	c.server()
	return &c
}
