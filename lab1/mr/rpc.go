package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Taskid string

type Taskstatus uint8

const (
	IDLE     Taskstatus = 0
	RUNNING  Taskstatus = 1
	FINISHED Taskstatus = 2
)

type Tasktype bool

const (
	MAPTASK    Tasktype = true
	REDUCETASK Tasktype = false
)

type Task struct {
	TaskID             Taskid
	TaskType           Tasktype
	MapTaskArgument    string
	ReduceTaskArgument []string
}

type TaskRelated struct {
	TaskStatus Taskstatus
	WorkerID   string
}

type WorkerInfo struct {
	WorkerID string
}

type GetTaskArgs struct {
	WorkerInfo WorkerInfo
}

type GetTaskReply struct {
	PleaseExit   bool
	HaveTask     bool
	ReduceNumber int
	Task         Task
}

type ReportTaskArgs struct {
	WorkerInfo       WorkerInfo
	TaskID           Taskid
	TaskType         Tasktype
	MapTaskResult    []string
	ReduceTaskResult string
}

type ReportTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
