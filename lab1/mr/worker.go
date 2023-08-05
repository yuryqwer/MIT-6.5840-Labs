package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"lab1/mrapps"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func createTmpFile(r int, dir string) ([]*os.File, error) {
	result := make([]*os.File, 0, r)
	for i := 0; i < r; i++ {
		f, err := os.CreateTemp(dir, strconv.Itoa(i))
		if err != nil {
			for _, file := range result {
				file.Close()
			}
			return nil, err
		}
		result = append(result, f)
	}
	return result, nil
}

func doMapTask(reply *GetTaskReply,
	mapf func(string, string) []mrapps.KeyValue,
	workerInfo WorkerInfo) error {
	filename := reply.Task.MapTaskArgument
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	files, err := createTmpFile(reply.ReduceNumber, ".")
	if err != nil {
		return err
	}
	encs := make([]*json.Encoder, 0, reply.ReduceNumber)
	for _, file := range files {
		encs = append(encs, json.NewEncoder(file))
		defer func(f *os.File) { f.Close() }(file)
	}
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		index := ihash(kv.Key) % reply.ReduceNumber
		err = encs[index].Encode(&kv)
	}
	mapTaskResult := make([]string, 0, reply.ReduceNumber)
	for i, file := range files {
		newpath := fmt.Sprintf("./mr-%s-%d", filepath.Base(reply.Task.MapTaskArgument), i)
		os.Rename(file.Name(), newpath)
		mapTaskResult = append(mapTaskResult, newpath)
	}
	args := ReportTaskArgs{
		WorkerInfo:    workerInfo,
		TaskID:        reply.Task.TaskID,
		TaskType:      reply.Task.TaskType,
		MapTaskResult: mapTaskResult,
	}
	reportTaskReply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reportTaskReply)
	if !ok {
		return fmt.Errorf("master cannot reach")
	}
	return nil
}

func doReduceTask(reply *GetTaskReply,
	reducef func(string, []string) string,
	workerInfo WorkerInfo) error {
	kva := make([]mrapps.KeyValue, 0)
	for _, filename := range reply.Task.ReduceTaskArgument {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv mrapps.KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(mrapps.ByKey(kva))
	oname := fmt.Sprintf("mr-out-%s", reply.Task.TaskID)
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	args := ReportTaskArgs{
		WorkerInfo:       workerInfo,
		TaskID:           reply.Task.TaskID,
		TaskType:         reply.Task.TaskType,
		ReduceTaskResult: oname,
	}
	reportTaskReply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reportTaskReply)
	if !ok {
		return fmt.Errorf("master cannot reach")
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []mrapps.KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		workerID := os.Getppid()
		workerInfo := WorkerInfo{strconv.Itoa(workerID)}
		args := GetTaskArgs{WorkerInfo: workerInfo}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok || reply.PleaseExit {
			log.Println("master cannot reach")
			return
		} else {
			if reply.HaveTask {
				switch reply.Task.TaskType {
				case MAPTASK:
					err := doMapTask(&reply, mapf, workerInfo)
					if err != nil {
						return
					}
				case REDUCETASK:
					err := doReduceTask(&reply, reducef, workerInfo)
					if err != nil {
						return
					}
				}
			} else {
				time.Sleep(time.Second)
			}
		}
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
