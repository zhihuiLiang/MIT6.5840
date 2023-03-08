package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
//
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := rand.Int()
	for id >= 0 && id <= 100 {
		id = rand.Int()
	}
	fmt.Printf("workID: %v\n", id)

	var reply *DistributeJobReply
	for {
		reply = CallDistributeJob(id)
		job := reply.Job
		if job == MapWork {
			inputFileDir := reply.InputFileDir
			JobNum := reply.JobNum
			nReduce := reply.NReduce
			inputFile, err := os.Open(inputFileDir)
			if err != nil {
				log.Fatalf("cannot open %v", inputFileDir)
			}
			content, err := ioutil.ReadAll(inputFile)
			if err != nil {
				log.Fatalf("cannot read %v", inputFileDir)
			}
			inputFile.Close()

			kvs := mapf(inputFileDir, string(content))
			sort.Sort(ByKey(kvs))

			thisDir := strconv.Itoa(id)
			err = os.MkdirAll(thisDir, os.ModePerm)
			if err != nil {
				panic(err)
			}

			dividedKv := make([][]KeyValue, nReduce)
			var kvNo int
			for _, kv := range kvs {
				kvNo = ihash(kv.Key) % nReduce
				dividedKv[kvNo] = append(dividedKv[kvNo], kv)
			}

			for i := 0; i < nReduce; i++ {
				intermediateFileName := fmt.Sprintf("%v/mr-%v-%v.json", thisDir, JobNum, i)
				intermediateFile, err := os.Create(intermediateFileName)
				if err != nil {
					panic(err)
				}

				encoder := json.NewEncoder(intermediateFile)
				err = encoder.Encode(dividedKv[i])
				intermediateFile.Close()
				if err != nil {
					panic(err)
				}
			}
			CallSubmitJob(id, MapWork, JobNum)
		} else if job == ReduceWork {
			JobNum := reply.JobNum
			mapTasks := reply.MapTasks
			thisDir := strconv.Itoa(id)
			err := os.MkdirAll(thisDir, os.ModePerm)
			if err != nil {
				panic(err)
			}
			var intermediateKvs []KeyValue
			for i, dirID := range mapTasks {
				toReadFileName := fmt.Sprintf("%v/mr-%v-%v.json", dirID, i, JobNum)
				intermediateFile, err := os.Open(toReadFileName)
				if err != nil {
					panic(err)
				}
				decoder := json.NewDecoder(intermediateFile)
				var tempKvs []KeyValue
				err = decoder.Decode(&tempKvs)
				intermediateFile.Close()
				intermediateKvs = append(intermediateKvs, tempKvs...)
			}
			sort.Sort(ByKey(intermediateKvs))

			outputFileName := fmt.Sprintf("mr-out-%v", JobNum)
			outputFile, _ := os.Create(outputFileName)

			i := 0
			for i < len(intermediateKvs) {
				j := i + 1
				for j < len(intermediateKvs) && intermediateKvs[j].Key == intermediateKvs[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, intermediateKvs[k].Value)
				}
				output := reducef(intermediateKvs[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputFile, "%v %v\n", intermediateKvs[i].Key, output)
				i = j
			}

			outputFile.Close()
			CallSubmitJob(id, ReduceWork, JobNum)
		} else {
			fmt.Printf("no Job, sleep for 3s\n")
			time.Sleep(3 * time.Second)
		}
	}
}

func CallDistributeJob(id int) *DistributeJobReply {
	args := DistributeJobArgs{}
	reply := DistributeJobReply{}
	args.WorkerID = id

	var ok bool
	ok = call("Coordinator.DistributeJob", &args, &reply)
	if ok != true {
		fmt.Printf("apply a Job failed!\n")
	}
	return &reply
}

func CallSubmitJob(id int, job string, jobNum int) {

	args := SubmitJobArgs{id, job, jobNum}
	reply := SubmitJobReply{}

	ok := call("Coordinator.SubmitJob", &args, &reply)
	if !ok {
		fmt.Printf("submit job failed!\n")
	}
}

// CallExample
//
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

	fmt.Println("Dial Failed.", err)
	return false
}
