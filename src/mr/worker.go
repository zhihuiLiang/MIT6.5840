package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
			args := TaskRequestArgs{RequestMap}
			reply := TaskRequestReply{}
			ok := call("Coordinator.TaskRequest", &args, &reply)
			if !ok {
				log.Fatalf("Call Rpc TaskRequest Failed!")
			}
			DoMapTask(mapf, reply.Filename, reply.WorkerID, reply.ReduceNum)
		case WaitTaskState:

		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(mapf func(string, string) []KeyValue, filename string, workID int32, nReduce int32) {
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
	for _, kv := range kva {
		index := ihash(kv.Key) % int(nReduce)
		outputName := fmt.Sprintf("mr-tmp-%d-%d", workID, index)
		file, err := os.OpenFile(outputName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("cannot open file %v", outputName)
		}
		file.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		file.Close()
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
