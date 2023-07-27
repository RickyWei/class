package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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
	doMap := func() {
		// Step 1 -> Ask coordinator for a map task
		askReq, askResp := &AskMapTaskRequest{}, &AskMapTaskResponse{}
		if !call("Coordinator.AskMap", askReq, askResp) || askResp.Task == nil {
			return
		}
		// Step 2 -> handle map and notify coordinator the finish of asked map task
		m := make(map[int][]KeyValue)
		for _, filename := range askResp.Task.FileNames {
			file, err := os.Open(filename)
			if err != nil {
				return
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				return
			}
			kva := mapf(filename, string(content))
			for i, kv := range kva {
				h := ihash(kv.Key) % askResp.Task.NReduce
				m[h] = append(m[h], kva[i])
			}
		}
		respFileNames := make([]string, 0)
		for h, kvs := range m {
			bs, err := json.Marshal(kvs)
			if err != nil {
				return
			}
			if err = os.WriteFile(fmt.Sprintf("mr-%d-%d", askResp.Task.ID, h), bs, 0644); err != nil {
				return
			}
			respFileNames = append(respFileNames, fmt.Sprintf("mr-%d-%d", askResp.Task.ID, h))
		}
		call("Coordinator.FinishMap", &FinishMapTaskRequest{MapTaskID: askResp.Task.ID, FileNames: respFileNames}, &FinishMapTaskResponse{})
	}
	doReduce := func() {
		// Step 1 -> Ask coordinator for a reduce task
		askReq, askResp := &AskReduceTaskRequest{}, &AskReduceTaskResponse{}
		if !call("Coordinator.AskReduce", askReq, askResp) || askResp.Task == nil {
			return
		}
		// Step 2 -> handle reduce and notify coordinator the finish of asked map task
		intermediate := make([]KeyValue, 0)
		for _, filename := range askResp.Task.FileNames {
			im := make([]KeyValue, 0)
			f, err := os.Open(filename)
			if err != nil {
				return
			}
			bs, err := ioutil.ReadAll(f)
			if err != nil {
				return
			}
			if json.Unmarshal(bs, &im) != nil {
				return
			}
			intermediate = append(intermediate, im...)
		}
		sort.Slice(intermediate, func(i, j int) bool { return intermediate[i].Key < intermediate[j].Key })
		ofile, err := os.Create(fmt.Sprintf("mr-out-%d", askResp.Task.ID))
		if err != nil {
			return
		}
		defer ofile.Close()
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			if _, err = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output); err != nil {
				return
			}

			i = j
		}

		call("Coordinator.FinishReduce", &FinishReduceTaskRequest{ReduceTaskID: askResp.Task.ID}, &FinishMapTaskResponse{})
	}

	for {
		doMap()
		doReduce()
		// exit if all tasks are done
		isDone := false
		call("Coordinator.IsDone", &isDone, &isDone)
		if isDone {
			return
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
