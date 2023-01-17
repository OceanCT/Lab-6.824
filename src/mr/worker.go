package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	// Your worker implementation here.
    workerId := -1
    done := false
    var getWorkerIdDuration time.Duration
    for !done {
        if workerId == -1 {
            workerId = GetWorkerID(getWorkerIdDuration)
        } else {

            flag,task := CallPullTask(workerId)
            if flag == -1 {
                workerId = -1
            } else if flag == 0 {
                done = true
            } else if task.taskType == MapTask{
            
            } else if task.taskType == ReduceTask {

            }
        }   
    } 

}
// this function gets a worker id from the coordinator
func GetWorkerID(duration time.Duration) int {
    var args    int
    var workerId int
    ok := false 
    for !ok {
        ok = call("Coordinator.RegisterWorker", &args, &workerId)
        time.Sleep(duration)
    }
    return workerId
}

//
// The function pulls task from the coordinator
// return 1 if there exist a task needed to be done
// return 0 if everything is over
// return -1 if the workerId is no long valid
func CallPullTask(workerId int) (flag int, task *Task) {
    getTaskReq := GetTaskReq {}
    ok := false
    for !ok {
        ok = call("Coordinator.PullTask", workerId, &getTaskReq)
    }
    if !getTaskReq.workerState {
        return -1, nil
    } else if getTaskReq.task == nil{
        return 0, nil
    } else {
        return 1, getTaskReq.task
    }
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
