package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	FileNames         []string
	NReduce           int
	IsAllDone         atomic.Bool
	mtx               sync.RWMutex
	MapTasks          []*Task
	IntermediateFiles []string
	ReduceTasks       []*Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskMap(req *AskMapTaskRequest, resp *AskMapTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, t := range c.MapTasks {
		if t.Status == 0 || (t.Status == 1 && time.Since(t.AssignTime).Seconds() > 10) {
			resp.Task = t
			t.Status = 1
			t.AssignTime = time.Now()
			break
		}
	}

	return nil
}
func (c *Coordinator) FinishMap(req *FinishMapTaskRequest, resp *FinishMapTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, t := range c.MapTasks {
		if t.ID == req.MapTaskID {
			t.Status = 2
			c.IntermediateFiles = append(c.IntermediateFiles, req.FileNames...)
		}
	}

	return nil
}
func (c *Coordinator) AskReduce(req *AskReduceTaskRequest, resp *AskReduceTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, t := range c.ReduceTasks {
		if t.Status == 0 || (t.Status == 1 && time.Since(t.AssignTime).Seconds() > 10) {
			resp.Task = t
			t.Status = 1
			t.AssignTime = time.Now()
			break
		}
	}

	return nil
}
func (c *Coordinator) FinishReduce(req *FinishReduceTaskRequest, resp *FinishReduceTaskResponse) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, t := range c.ReduceTasks {
		if t.ID == req.ReduceTaskID {
			t.Status = 2
		}
	}

	return nil
}

func (c *Coordinator) IsDone(req *bool, resp *bool) error {
	*resp = c.IsAllDone.Load()
	return nil
}

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := true

	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, t := range c.MapTasks {
		if t.Status != 2 {
			ret = false
		}
	}

	if ret && c.ReduceTasks == nil {
		m := make(map[int][]string)
		for _, f := range c.IntermediateFiles {
			hs := strings.Split(f, "-")[2]
			h, err := strconv.Atoi(hs)
			if err != nil {
				fmt.Println(err)
			}
			m[h] = append(m[h], f)
		}

		for k, v := range m {
			c.ReduceTasks = append(c.ReduceTasks, &Task{
				ID:         k,
				Status:     0,
				FileNames:  v,
				AssignTime: time.Time{},
			})
		}
	}

	for _, t := range c.ReduceTasks {
		if t.Status != 2 {
			ret = false
		}
	}

	// wait to response workders
	if ret {
		c.IsAllDone.Store(true)
		time.Sleep(time.Second * 3)
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileNames = files
	c.NReduce = nReduce
	chunkNum := len(c.FileNames) / c.NReduce
	if chunkNum <= 0 {
		chunkNum = 1
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for i := 0; i < len(c.FileNames); i += chunkNum {
		c.MapTasks = append(c.MapTasks, &Task{ID: i, FileNames: c.FileNames[i : i+chunkNum], NReduce: nReduce})
	}

	c.server()
	return &c
}
