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

type Coordinator struct {
	// Your definitions here.
	workerNum      int
	workerState    map[int]bool
	workerLastPing map[int]time.Time
	taskNum        int
	taskInfo       map[int]Task
	// to which worker the task belong to
	taskBelong map[int]int
	// which tasks a worker is doing
	workerHold map[int][]int
	// to which task the input file belongs to
	checkStateDuration time.Duration
	// if a worker fails to refresh its workerState in a certain period of time
	// it is considered as dead
	stateLength      time.Duration
	pullTaskDuration time.Duration
	// whether map tasks has failed
	mapTaskDone bool
	// to which reduce task the map task belong to
	mapTaskBelong           map[int]int
	undistributedMapTask    []int
	undistributedReduceTask []int
	reduceTaskFinished      map[int]bool
	done                    bool
	mutex                   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Whenever a worker decides to be join the group, it register itself and get a unique workerId
func (c *Coordinator) RegisterWorker(args interface{}, workerId *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	workerId = &c.workerNum
	c.workerNum++
	c.workerLastPing[*workerId] = time.Now()
	return nil
}

// Periodically, a worker reminds coordinator that it is still alive,
// if coodinator did not receive the reply from a worker for a long time,
// the worker is sentenced to death and it can only join the group again
// by re-registering
func (c *Coordinator) RefreshWorkerState(workerId int, newWorkerId *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.workerState[workerId] {
		newWorkerId = &c.workerNum
		c.workerNum++
		c.workerLastPing[*newWorkerId] = time.Now()
	} else {
		newWorkerId = &workerId
		c.workerLastPing[*newWorkerId] = time.Now()
	}
	return nil
}

// Periodically, the coordinator check whether there are workers dead already
func (c *Coordinator) CheckWorkerState() {
	go func() {
		for true {
			time.Sleep(c.checkStateDuration)
			c.mutex.Lock()
			for workerId, _ := range c.workerHold {
				if !c.workerState[workerId] {
					continue
				}
				if c.workerLastPing[workerId].Add(c.stateLength).Before(time.Now()) {
					c.workerState[workerId] = false
					c.HandleIllegalWorker(workerId)
				}
			}
			c.mutex.Unlock()
		}
	}()
}

// if a illegalWorker is discovered, the coordinator reallocate its tasks to other workers
// !!! Lock is not written here, since this function is supposed to be used in another function
// that is safe so as to avoid dead locks.
func (c *Coordinator) HandleIllegalWorker(illegalWorker int) {
	redo := func(taskId int) {
		taskInfo := c.taskInfo[taskId]
		if taskInfo.taskType == ReduceTask {
			c.undistributedReduceTask = append(c.undistributedReduceTask, taskId)
		} else {
			if !c.reduceTaskFinished[c.mapTaskBelong[taskId]] {
				c.undistributedMapTask = append(c.undistributedMapTask, taskId)
			}
		}
	}
	for _, taskId := range c.workerHold[illegalWorker] {
		redo(taskId)
	}
}

// a worker pull a task identified by its workerId, if the coordinator recognize it as dead,
// the worker has to redo the registeration
func (c *Coordinator) PullTask(workerId int, getTaskReq *GetTaskReq) error {
	for true {
		c.mutex.Lock()
		// check if workerId is still valid
		if !c.workerState[workerId] {
			getTaskReq.workerState = false
			break
		} else if c.done {
			getTaskReq.workerState = true
			getTaskReq.task = nil
			break
		} else if len(c.undistributedMapTask) > 0 {
			getTaskReq.workerState = true
			*getTaskReq.task = c.taskInfo[c.undistributedMapTask[0]]
			c.workerHold[workerId] = append(c.workerHold[workerId], c.undistributedMapTask[0])
			c.taskBelong[c.undistributedMapTask[0]] = workerId
			c.undistributedMapTask = c.undistributedMapTask[1:]
			break
		} else if len(c.undistributedReduceTask) > 0 {
			getTaskReq.workerState = true
			*getTaskReq.task = c.taskInfo[c.undistributedReduceTask[0]]
			c.taskBelong[c.undistributedReduceTask[0]] = workerId
			c.undistributedReduceTask = c.undistributedReduceTask[1:]
			break
		} else if !c.done {
			c.mutex.Unlock()
			time.Sleep(c.pullTaskDuration)
		}
	}
	c.mutex.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		taskNum: len(files) + nReduce,
	}
	// Your code here.

	c.server()
	return &c
}
