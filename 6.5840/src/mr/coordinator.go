package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAPTASK = iota + 1
	REDUCETASK
	EXIT
)

type Coordinator struct {
	// Your definitions here.
	nReduce   int
	nMap      int
	TaskQueue chan *Task
	TaskMap   map[int]*Task
	Type      uint
	mu        sync.Mutex
}

type Task struct {
	TaskId     int
	File       string
	TaskType   uint
	NReduce    int
	NMap       int
	CreateTime int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if len(c.TaskQueue) > 0 {
		reply.Task = <-c.TaskQueue
		// fmt.Printf("reply %v\n", *reply.Task)
	} else {
		reply.Task = &Task{}
	}
	return nil
}

func (c *Coordinator) RequestTaskDone(args *RequestTaskDoneArgs, reply *RequestTaskDoneReply) error {
	c.mu.Lock()
	delete(c.TaskMap, args.Id)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ChangeWorkType() {
	for {
		c.mu.Lock()
		if c.Type == MAPTASK && len(c.TaskMap) == 0 {
			c.Type = REDUCETASK
			for i := 0; i < c.nReduce; i++ {
				taskTemp := Task{
					TaskId:     i,
					TaskType:   REDUCETASK,
					NReduce:    c.nReduce,
					NMap:       c.nMap,
					CreateTime: int(time.Now().Unix()),
				}
				c.TaskMap[i] = &taskTemp
				c.TaskQueue <- &taskTemp
			}
		}
		if c.Type == REDUCETASK && len(c.TaskMap) == 0 {
			c.Type = EXIT
			for i := 0; i < c.nReduce; i++ {
				taskTemp := Task{
					TaskId:     i,
					TaskType:   EXIT,
					NReduce:    c.nReduce,
					NMap:       c.nMap,
					CreateTime: int(time.Now().Unix()),
				}
				c.TaskMap[i] = &taskTemp
				c.TaskQueue <- &taskTemp
			}
		}
		c.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.Type == EXIT {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

func (c *Coordinator) CheckTimeOut() {
	for {
		c.mu.Lock()
		for _, v := range c.TaskMap {
			if time.Now().Unix()-int64(v.CreateTime) > 10 {
				v.CreateTime = int(time.Now().Unix())
				c.TaskQueue <- v
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Second * 5)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.TaskQueue = make(chan *Task, nReduce)
	c.TaskMap = make(map[int]*Task)
	c.Type = MAPTASK
	c.mu = sync.Mutex{}
	for i, file := range files {
		taskTemp := Task{
			TaskId:     i,
			File:       file,
			TaskType:   MAPTASK,
			NReduce:    c.nReduce,
			NMap:       c.nMap,
			CreateTime: int(time.Now().Unix()),
		}
		c.TaskMap[i] = &taskTemp
		c.TaskQueue <- &taskTemp
	}
	go c.ChangeWorkType()
	go c.CheckTimeOut()
	c.server()
	return &c
}
