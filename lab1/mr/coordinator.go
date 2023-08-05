package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTask                 map[Taskid]*TaskRelated
	reduceTask              map[Taskid]*TaskRelated
	reduceTaskFile          map[Taskid][]string
	mapTaskFinishedCount    int
	reduceTaskFinishedCount int
	reduceNumber            int
	mutex                   sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.ReduceNumber = c.reduceNumber
	reply.PleaseExit = false
	if c.reduceTaskFinishedCount == c.reduceNumber {
		reply.PleaseExit = true
		return nil
	}
	if c.mapTaskFinishedCount == len(c.mapTask) { // All map tasks finished
		for k := range c.reduceTask {
			status := c.reduceTask[k].TaskStatus
			if status == RUNNING || status == FINISHED {
				continue
			}
			reply.HaveTask = true
			reply.Task.TaskID = k
			reply.Task.TaskType = REDUCETASK
			reply.Task.ReduceTaskArgument = append(reply.Task.ReduceTaskArgument, c.reduceTaskFile[k]...)
			c.reduceTask[k].TaskStatus = RUNNING
			c.reduceTask[k].WorkerID = args.WorkerInfo.WorkerID
			go c.handleTimeout(k, REDUCETASK)
			return nil
		}
		// No reduce tasks can be executed
		reply.HaveTask = false
		return nil
	} else {
		for k := range c.mapTask {
			status := c.mapTask[k].TaskStatus
			if status == RUNNING || status == FINISHED {
				continue
			}
			reply.HaveTask = true
			reply.Task.TaskID = k
			reply.Task.TaskType = MAPTASK
			reply.Task.MapTaskArgument = string(k)
			c.mapTask[k].TaskStatus = RUNNING
			c.mapTask[k].WorkerID = args.WorkerInfo.WorkerID
			go c.handleTimeout(k, MAPTASK)
			return nil
		}
		// No map tasks can be executed
		reply.HaveTask = false
		return nil
	}
}

func (c *Coordinator) handleTimeout(taskid Taskid, tasktype Tasktype) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch tasktype {
	case MAPTASK:
		if c.mapTask[taskid].TaskStatus == RUNNING {
			c.mapTask[taskid].TaskStatus = IDLE
			c.mapTask[taskid].WorkerID = ""
		} else {

		}
	case REDUCETASK:
		if c.reduceTask[taskid].TaskStatus == RUNNING {
			c.reduceTask[taskid].TaskStatus = IDLE
			c.reduceTask[taskid].WorkerID = ""
		} else {

		}
	}
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch args.TaskType {
	case MAPTASK:
		// the task is running by this worker or someone else
		if t := c.mapTask[args.TaskID]; t.TaskStatus == RUNNING {
			// if the task's Workerid differs from the one in the args
			// it does not matter because the result of the task should be same
			// just take this result and ignore the later result
			c.mapTask[args.TaskID].TaskStatus = FINISHED
			c.mapTask[args.TaskID].WorkerID = args.WorkerInfo.WorkerID // I finish it first
			c.mapTaskFinishedCount++
			for _, filePath := range args.MapTaskResult {
				index := strings.LastIndex(filePath, "-")
				reduceTaskID := Taskid(filePath[index+1:])
				c.reduceTaskFile[reduceTaskID] = append(c.reduceTaskFile[reduceTaskID], filePath)
			}
		} else { // task timeout and is waiting to be executed or finished by others
			log.Printf("sorry worker %s, it's too late\n", args.WorkerInfo.WorkerID)
		}
	case REDUCETASK:
		if t := c.reduceTask[args.TaskID]; t.TaskStatus == RUNNING {
			c.reduceTask[args.TaskID].TaskStatus = FINISHED
			c.reduceTask[args.TaskID].WorkerID = args.WorkerInfo.WorkerID
			c.reduceTaskFinishedCount++
		} else {
			log.Printf("sorry worker %s, it's too late\n", args.WorkerInfo.WorkerID)
		}
	}
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
	ret := false

	// Your code here.
	if c.reduceTaskFinishedCount == c.reduceNumber {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTask = make(map[Taskid]*TaskRelated)
	for _, file := range files {
		c.mapTask[Taskid(file)] = &TaskRelated{IDLE, ""}
	}
	c.reduceTask = make(map[Taskid]*TaskRelated)
	for i := 0; i < nReduce; i++ {
		c.reduceTask[Taskid(strconv.Itoa(i))] = &TaskRelated{IDLE, ""}
	}
	c.reduceTaskFile = make(map[Taskid][]string)
	c.reduceNumber = nReduce
	c.server()
	return &c
}
