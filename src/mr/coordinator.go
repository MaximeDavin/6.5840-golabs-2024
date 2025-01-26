package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	id        int
	filenames []string
	done      bool
	assigned  bool
	startedAt time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	timeout     time.Duration
	nReduce     int
	nMap        int
	mapTasks    map[int]Task
	reduceTasks map[int]Task
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd, task := c.getNextTask()

	reply.Command = cmd
	if cmd == MAP {
		reply.TaskId = task.id
		reply.Filenames = task.filenames
		reply.NReduce = c.nReduce
	} else if cmd == REDUCE {
		reply.TaskId = task.id
		reply.NMap = c.nMap
	}

	return nil
}

func (c *Coordinator) SetTaskDone(args *SetTaskDoneArgs, reply *SetTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Command == MAP {
		t := c.mapTasks[args.TaskId]
		t.done = true
		c.mapTasks[args.TaskId] = t
	} else if args.Command == REDUCE {
		t := c.reduceTasks[args.TaskId]
		t.done = true
		c.reduceTasks[args.TaskId] = t
	} else {
		return errors.New("unknown command")
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return areTasksDone(c.mapTasks) && areTasksDone(c.reduceTasks)
}

func hasTimedout(startedAt time.Time, timeout time.Duration) bool {
	t := time.Now()
	elapsed := t.Sub(startedAt)
	return elapsed > timeout
}

func getTaskToDo(tasks map[int]Task, timeout time.Duration) *Task {
	for _, task := range tasks {
		if !task.done && (!task.assigned || hasTimedout(task.startedAt, timeout)) {
			task.assigned = true
			task.startedAt = time.Now()
			tasks[task.id] = task
			return &task
		}
	}
	return nil
}

func areTasksDone(tasks map[int]Task) bool {
	for _, task := range tasks {
		if !task.done {
			return false
		}
	}
	return true
}

func (c *Coordinator) getNextTask() (Cmd, *Task) {
	// We try to find a map task that is not assigned or has timeouted
	mapTask := getTaskToDo(c.mapTasks, c.timeout)
	if mapTask != nil {
		return MAP, mapTask
	}
	// If there is still a map task that is not done, we tell the worker
	// to wait and retry later
	if !areTasksDone(c.mapTasks) {
		return WAIT, nil // Try again later
	}

	// All map tasks are done, we can start reduce tasks
	// We try to find a reduce task that is not assigned or has timedouted
	reduceTask := getTaskToDo(c.reduceTasks, c.timeout)
	if reduceTask != nil {
		return REDUCE, reduceTask
	}
	// If there is still a reduce task that is not done, we tell the worker
	// to wait and retry later
	if !areTasksDone(c.reduceTasks) {
		return WAIT, nil // Try again later
	}

	// We tell the worker to exit gracefully
	return EXIT, nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		timeout:     10 * time.Second,
		nReduce:     nReduce,
		nMap:        len(files),
		mapTasks:    make(map[int]Task),
		reduceTasks: make(map[int]Task),
	}

	// Initialize map tasks
	for i, filename := range files {
		t := Task{
			id:        i,
			filenames: []string{filename},
			done:      false,
			assigned:  false,
		}
		c.mapTasks[i] = t
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		t := Task{
			id:       i,
			done:     false,
			assigned: false,
		}
		c.reduceTasks[i] = t
	}

	c.server()
	return c
}
