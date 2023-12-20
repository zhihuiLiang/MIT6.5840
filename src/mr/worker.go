package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	MapTaskState    = 1
	WaitTaskState   = 2
	ReduceTaskState = 3
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.
	hasFinish := false
	taskState := MapTaskState

	for !hasFinish {
		switch taskState {
		case MapTaskState:
			args := MapTaskRequestArgs{}
			reply := MapTaskRequestReply{}
			ok := call("Coordinator.MapTaskRequest", &args, &reply)
			if !ok {
				log.Fatalf("Call Rpc TaskRequest Failed!")
			}
			if reply.HasMapTask {
				DoMapTask(mapf, reply.Filename, reply.MapTaskID, reply.ReduceNum)
				ok := call("Coordinator.MapDoneRequest", &MapDoneTaskRequestArgs{}, &MapDoneTaskRequestReply{})
				if !ok {
					log.Fatalf("Call Rpc MapDoneRequest Failed!")
				}
			} else {
				taskState = WaitTaskState
			}
		case WaitTaskState:
			reply := GoReduceRequestReply{}
			ok := call("Coordinator.GoReduceRequest", &GoReduceRequestArgs{}, &reply)
			if !ok {
				log.Fatalf("Call GoReduceRequest Failed!")
			}
			if reply.Ok {
				taskState = ReduceTaskState
			}
		case ReduceTaskState:
			args := ReduceTaskRequestArgs{}
			reply := ReduceTaskRequestReply{}
			ok := call("Coordinator.ReduceTaskRequest", &args, &reply)
			if !ok {
				log.Fatalf("Call Rpc TaskRequest Failed!")
			}
			DoReduceTask(reply.ReduceTaskID, reply.MaxMapTaskID, reducef)
			hasFinish = true
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(mapf func(string, string) []KeyValue, filename string, taskID int32, nReduce int32) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()

	kva := mapf(filename, string(content))
	hashKV := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % int(nReduce)
		hashKV[index] = append(hashKV[index], kv)
	}
	for i := 0; i < int(nReduce); i++ {
		outputName := fmt.Sprintf("mr-tmp-%d-%d", taskID, i)
		file, err := os.OpenFile(outputName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot open file %v", outputName)
		}
		enc := json.NewEncoder(file)
		for _, kv := range hashKV[i] {
			enc.Encode(&kv)
		}
		file.Close()
	}
}

func DoReduceTask(id int32, maxMapTaskID int32, reducef func(string, []string) string) {
	var kva []KeyValue
	for mapTaskID := 0; mapTaskID < int(maxMapTaskID); mapTaskID++ {
		filename := fmt.Sprintf("mr-tmp-%d-%d", mapTaskID, id)
		_, err := os.Lstat(filename)
		if !os.IsNotExist(err) {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Open %s Failed! Err:%s", filename, err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
