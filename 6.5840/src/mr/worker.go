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

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := CallTask()
		switch task.TaskType {
		case MAPTASK:
			// log.Println("map task")
			if err := DoMapWork(task, mapf); err != nil {
				fmt.Println(err.Error())
			}
			CallWorkDone(task.TaskId)
		case REDUCETASK:
			// log.Println("reduce task")
			if err := DoReduceWork(task, reducef); err != nil {
				fmt.Println(err.Error())
			}
			// log.Println("reduce task over")
			CallWorkDone(task.TaskId)
		case 0:
			// log.Println("no task")
			time.Sleep(100 * time.Millisecond)
			// os.Exit(0)
		case EXIT:
			// log.Println("exit")
			os.Exit(0)
		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallTask() *Task {
	args := RequestTaskArgs{}

	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		// fmt.Printf("reply %v\n", *reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Task
}

func CallWorkDone(id int) {
	args := RequestTaskDoneArgs{
		Id: id,
	}

	reply := RequestTaskDoneReply{}

	ok := call("Coordinator.RequestTaskDone", &args, &reply)
	if ok {
		// fmt.Printf("call RequestTaskDone \n")
	} else {
		fmt.Printf("call RequestTaskDone failed!\n")
	}
}
func DoReduceWork(task *Task, reducef func(string, []string) string) error {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < task.NMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Println("open file fail!")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	// 删除文件
	for i := 0; i < task.NMap; i++ {
		os.Remove(fmt.Sprintf("mr-%d-%d", i, task.TaskId))
	}
	return nil
}

func DoMapWork(task *Task, mapf func(string, string) []KeyValue) error {
	intermediates := make([][]KeyValue, task.NReduce)
	for i := 0; i < len(intermediates); i++ {
		intermediates[i] = make([]KeyValue, 0)
	}
	file, err := os.Open(task.File)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	kva := mapf(task.File, string(content))
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % task.NReduce
		intermediates[reduceId] = append(intermediates[reduceId], kv)
	}
	for i := 0; i < len(intermediates); i++ {
		tempFileNmae := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		tempFile, err := os.Create(tempFileNmae)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediates[i] {
			if err := enc.Encode(kv); err != nil {
				return err
			}
		}
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
