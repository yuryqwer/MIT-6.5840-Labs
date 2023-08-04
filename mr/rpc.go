package mr

import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

const (
	TaskServiceName = "TaskService"
	FileServiceName = "FileService"
)

type tasktype string

const (
	MAPTASK    tasktype = "map"
	REDUCETASK tasktype = "reduce"
)

type GetTaskRequest struct {
	WorkerID string
}

type GetTaskReply struct {
	PleaseExit bool
	NoTaskNow  bool
	TaskID     string
	TaskType   tasktype
	MapTaskArg []struct {
		FileName    string
		FileContent string
	}
	ReduceTaskArg []struct {
		Address  string
		FilePath string
	}
}

type ReportTaskRequest struct {
	WorkerID      string
	TaskID        string
	TaskType      tasktype
	MapTaskResult []struct {
		Address  string
		FilePath string
	}
	ReduceTaskResult struct {
		Address  string
		FilePath string
	}
}

type ReportTaskReply struct {
}

type GetFileRequest struct {
	WorkerID string
	FilePath string
}

type GetFileReply struct {
	FileContent string
}

type TaskServiceInterface interface {
	GetTask(request *GetTaskRequest, reply *GetTaskReply) error
	ReportTask(request *ReportTaskRequest, reply *ReportTaskReply) error
}

type FileServiceInterface interface {
	GetFile(request *GetFileRequest, reply *GetFileReply) error
}

func RegisterTaskService(svc TaskServiceInterface) error {
	return rpc.RegisterName(TaskServiceName, svc)
}

func RegisterFileService(svc FileServiceInterface) error {
	return rpc.RegisterName(FileServiceName, svc)
}

type TaskServiceClient struct {
	*rpc.Client
}

func DialTaskService(network, address string) (*TaskServiceClient, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
	return &TaskServiceClient{Client: client}, nil
}

func (t *TaskServiceClient) GetTask(
	request *GetTaskRequest,
	reply *GetTaskReply,
) error {
	return t.Client.Call(TaskServiceName+".GetTask", request, reply)
}

func (t *TaskServiceClient) ReportTask(
	request *ReportTaskRequest,
	reply *ReportTaskReply,
) error {
	return t.Client.Call(TaskServiceName+".ReportTask", request, reply)
}

type FileServiceClient struct {
	*rpc.Client
}

func DialFileService(network, address string) (*FileServiceClient, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	client := rpc.NewClientWithCodec(jsonrpc.NewClientCodec(conn))
	return &FileServiceClient{Client: client}, nil
}

func (t *TaskServiceClient) GetFile(request *GetFileRequest, reply *GetFileReply) error {
	return t.Client.Call(FileServiceName+".GetFile", request, reply)
}
