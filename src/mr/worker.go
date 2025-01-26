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

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	for {
		CallGetTask(mapf, reducef)
	}
}

func CallGetTask(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		cmd := reply.Command
		if cmd == WAIT {
			time.Sleep(time.Second)
		} else if cmd == EXIT {
			os.Exit(0)
		} else if cmd == MAP {
			doMapTask(mapf, reply.TaskId, reply.Filenames, reply.NReduce)
			CallSetTaskDone(MAP, reply.TaskId)
		} else if cmd == REDUCE {
			doReduceTask(reducef, reply.TaskId, reply.NMap)
			CallSetTaskDone(REDUCE, reply.TaskId)
		} else {
			os.Exit(1)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doMapTask(mapf func(string, string) []KeyValue, taskId int, filenames []string, nreduce int) {
	// apply map function to each file
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// sort intermediate key value pairs
	// sort.Sort(ByKey(intermediate))

	// partition intermediate key value pairs
	splitIntermediate := make([][]KeyValue, nreduce)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % nreduce
		splitIntermediate[idx] = append(splitIntermediate[idx], kv)
	}

	for i := 0; i < nreduce; i++ {
		name := fmt.Sprintf("mr-%v-%v", taskId, i)
		file, _ := os.Create(name)
		enc := json.NewEncoder(file)
		for _, kv := range splitIntermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		defer file.Close()
	}
}

func doReduceTask(reducef func(string, []string) string, taskId int, nmap int) {
	intermediate := []KeyValue{}
	for i := 0; i < nmap; i++ {
		name := fmt.Sprintf("mr-%v-%v", i, taskId)
		file, err := os.Open(name)
		if err != nil {
			log.Fatalf("cannot open %v", name)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			// append key value pairs to the intermediate list
			intermediate = append(intermediate, kv)
		}
	}

	// sort intermediate key value pairs
	sort.Sort(ByKey(intermediate))

	name := fmt.Sprintf("mr-out-%v", taskId)
	file, _ := os.Create(name)

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
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	defer file.Close()

}

func CallSetTaskDone(command Cmd, taskId int) {
	args := SetTaskDoneArgs{Command: command, TaskId: taskId}
	reply := SetTaskDoneReply{}
	ok := call("Coordinator.SetTaskDone", &args, &reply)
	if !ok {
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
