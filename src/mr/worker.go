package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	workerId int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w := &worker{
		workerId: getWorkerId(),
		mapf:     mapf,
		reducef:  reducef,
	}
	if w.workerId != -1 {
		DPrintf("worker:%d started", w.workerId)
		w.run()
	}
}

// this function gets a worker id from the coordinator
func getWorkerId() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	if ok := call("Coordinator.RegisterWorker", args, &reply); !ok {
		DPrintf("Fatal: worker failed to register")
		return -1
	}
	return reply.WorkerId
}

func (w *worker) pullTask() (Task, bool) {
	args := PullTaskArgs{
		WorkerId: w.workerId,
	}
	reply := PullTaskReply{}
	if ok := call("Coordinator.PullTask", args, &reply); !ok {
		DPrintf("worker fail to pull task")
		os.Exit(1)
	}
	return reply.Task, reply.Done
}

func (w *worker) reportTask(task Task, done bool) {
	args := ReportTaskArgs{
		WorkerId: w.workerId,
		Done:     done,
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	}
	reply := ReportTaskReply{}
	if ok := call("Coordinator.ReportTask", args, &reply); !ok {
		DPrintf("report Task fail:%+v", args)
	}
}

func (w *worker) doMapTask(t Task) {
    // fmt.Printf("doing MapTask: %d\n", t.TaskId)
    if contents, err := ioutil.ReadFile(t.InputFileName); err != nil {
		w.reportTask(t, false)
	} else {
		mapRes := w.mapf(t.InputFileName, string(contents))
		dividedMapResult := make([][]KeyValue, t.NReduce)
		for _, kv := range mapRes {
			reduceId := ihash(kv.Key) % t.NReduce
			dividedMapResult[reduceId] = append(dividedMapResult[reduceId], kv)
		}
		for reduceId, result := range dividedMapResult {
			fileName := getReduceName(t.TaskId, reduceId)
			f, err := os.Create(fileName)
			if err != nil {
				w.reportTask(t, false)
				return
			}
			enc := json.NewEncoder(f)
			for _, kv := range result {
				if err := enc.Encode(&kv); err != nil {
					w.reportTask(t, false)
                    return
				}
			}
			if err := f.Close(); err != nil {
				w.reportTask(t, false)
				return
			}
		}
	}
	w.reportTask(t, true)
}

func (w *worker) doReduceTask(t Task) {
	mapResult := make(map[string][]string)
	for mapId := 0; mapId < t.NMap; mapId++ {
		fileName := getReduceName(mapId, t.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false)
			return
		}
		dec := json.NewDecoder(file)
		for true {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mapResult[kv.Key] = append(mapResult[kv.Key], kv.Value)
		}
	}
	res := make([]string, len(mapResult))
	for k, v := range mapResult {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(getResultName(t.TaskId), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false)
	} else {
		w.reportTask(t, true)
	}
}

func (w *worker) run() {
	for {
		t, done := w.pullTask()
		if done {
			break
		}
		if t.TaskType == MapTask {
			w.doMapTask(t)
		} else {
			w.doReduceTask(t)
		}
	}
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
