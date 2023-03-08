package mr

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	inputs      []string
	nReduce     int
	mapTasks    []int // len == nMap . key=0 waiting; key=1 processing; others key= done by worker
	reduceTasks []int // same as above
	mapDone     bool
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) DistributeJob(args *DistributeJobArgs, reply *DistributeJobReply) error {
	hasMapDone := c.checkMapDone()
	if !hasMapDone {
		jobNum := c.getAJob(MapWork, args.WorkerID)
		if jobNum == -1 {
			msg := "checked map hadn't done but got no work\n"
			fmt.Printf(msg)
			reply.Job = NoJob
			return errors.New(msg)
		}
		reply.Job = MapWork
		reply.JobNum = jobNum
		reply.NReduce = c.nReduce
		reply.InputFileDir = c.inputs[jobNum]
		fmt.Printf("give %v %v to %v.\n", MapWork, jobNum, args.WorkerID)
		//fmt.Printf("reply: %v \n", reply)
		return nil
	}

	hasReduceDone := c.Done()
	if !hasReduceDone {
		jobNum := c.getAJob(ReduceWork, args.WorkerID)
		if jobNum == -1 {
			msg := "checked reduce hadn't done but got no work"
			fmt.Printf(msg)
			reply.Job = NoJob
			return errors.New(msg)
		}
		reply.Job = ReduceWork
		reply.JobNum = jobNum
		reply.NReduce = c.nReduce
		reply.MapTasks = c.mapTasks
		fmt.Printf("give %v %v to %v.\n", ReduceWork, jobNum, args.WorkerID)
		//fmt.Printf("reply: %v \n", reply)
		return nil
	}

	reply.Job = NoJob
	return nil
}

func (c *Coordinator) SubmitJob(args *SubmitJobArgs, reply *SubmitJobReply) error {
	if args.Job == MapWork {
		c.mu.Lock()
		c.mapTasks[args.JobNum] = args.WorkerID
		c.mu.Unlock()
		_ = c.checkMapDone()
		reply.Ok = true
	} else if args.Job == ReduceWork {
		c.mu.Lock()
		c.reduceTasks[args.JobNum] = args.WorkerID
		c.mu.Unlock()
		reply.Ok = true
	} else {
		msg := "submitting Job format wrong"
		fmt.Print(msg)
		return errors.New(msg)
	}
	//fmt.Printf("Condition after submit: %v \n", c)
	return nil
}

// private func. return -1 if no Job is applicable.
func (c *Coordinator) getAJob(jobType string, workerID int) int {
	var waitingJob []int
	var jobToDistribute int
	if jobType == MapWork {
		for i, task := range c.mapTasks {
			if task == 0 {
				waitingJob = append(waitingJob, i)
			}
		}
		if len(waitingJob) == 0 {
			return -1
		}
		c.mu.Lock()
		jobToDistribute = waitingJob[rand.Intn(len(waitingJob))]
		c.mapTasks[jobToDistribute] = 1
		c.mu.Unlock()
	} else if jobType == ReduceWork {
		for i, task := range c.reduceTasks {
			if task == 0 {
				waitingJob = append(waitingJob, i)
			}
		}
		if len(waitingJob) == 0 {
			return -1
		}
		c.mu.Lock()
		jobToDistribute = waitingJob[rand.Intn(len(waitingJob))]
		c.reduceTasks[jobToDistribute] = 1
		c.mu.Unlock()
	}
	return jobToDistribute
}

func (c *Coordinator) checkMapDone() bool {
	for _, task := range c.mapTasks {
		if task >= 0 && task <= 10 {
			return false
		}
	}
	c.mu.Lock()
	c.mapDone = true
	c.mu.Unlock()
	return true
}

// Example
//
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

// Done
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire Job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.reduceTasks {
		if task >= 0 && task <= 10 {
			return false
		}
	}
	return true
}

// MakeCoordinator
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.inputs = files
	c.nReduce = nReduce
	c.mapTasks = []int{}
	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, 0)
	}
	c.reduceTasks = []int{}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, 0)
	}
	c.mapDone = false

	go func() {
		for {
			fmt.Printf("[checker] Maptasks: %v\n", c.mapTasks)
			fmt.Printf("[checker] Reducetasks: %v\n", c.reduceTasks)

			if !c.checkMapDone() {
				c.mu.Lock()
				for key, task := range c.mapTasks {
					if task >= 1 && task < 10 {
						c.mapTasks[key]++
					} else if task == 10 {
						c.mapTasks[key] = 0
					}
				}
				c.mu.Unlock()
			} else if !c.Done() {
				c.mu.Lock()
				for key, task := range c.reduceTasks {
					if task >= 1 && task < 10 {
						c.reduceTasks[key]++
					} else if task == 10 {
						c.reduceTasks[key] = 0
					}
				}
				c.mu.Unlock()
			} else {
				fmt.Printf("[checker] done.")
				return
			}
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
