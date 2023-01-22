package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
//    "fmt"
)

type TaskStatus int

const (
	Ready    TaskStatus = 0
	Queuing  TaskStatus = 1
	Running  TaskStatus = 2
	Finished TaskStatus = 3
	Error    TaskStatus = 4
)

type TaskInfo struct {
	status    TaskStatus
	workerId  int
	beginTime time.Time
}

type Coordinator struct {
	// the original files
	files []string
	// the number of reduce Task
	nReduce int
	// Task info
	taskInfo  []TaskInfo
	done      bool
	workerNum int
	// store Task
	taskChannel       []Task
    // not necessary, just for debug
    // mapTaskRedoTime   int
	phase             TaskType
	checkTaskInterval time.Duration
	maxTaskRunTime    time.Duration
	pullTaskInterval  time.Duration
	mutex             sync.Mutex
}

// this function allows worker registers itself to coordinator and get a WorkerId
func (c *Coordinator) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.workerNum++
	reply.WorkerId = c.workerNum
	return nil
}

// this function allows worker to get Task from coordinator
func (c *Coordinator) PullTask(args PullTaskArgs, reply *PullTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for len(c.taskChannel) == 0 {
		c.mutex.Unlock()
		time.Sleep(c.pullTaskInterval)
		if c.Done() {
			reply.Done = true
			// since we unlock after the function finished
			c.mutex.Lock()
			return nil
		}
		c.mutex.Lock()
	}
	task := c.taskChannel[0]
	c.taskChannel = c.taskChannel[1:]
	c.taskInfo[task.TaskId].status = Running
	c.taskInfo[task.TaskId].workerId = args.WorkerId
	c.taskInfo[task.TaskId].beginTime = time.Now()
	reply.Task = task
	reply.Done = false
	DPrintf("worker:%d get Task:%d", args.WorkerId, task.TaskId)
	return nil
}

// this function allows worker to report whether the Task is finished successfully
func (c *Coordinator) ReportTask(args ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.phase != args.TaskType || c.taskInfo[args.TaskId].workerId != args.WorkerId {
		return nil
	}
	if !args.Done {
		c.taskInfo[args.TaskId].status = Error
	} else {
		c.taskInfo[args.TaskId].status = Finished
	}
	// after a Task status is changed, immediately start to check Task to avoid unnecessary wait
	go c.doCheckTask()
	return nil
}

func (c *Coordinator) doCheckTask() {
	c.mutex.Lock()
	// DPrintf("checking task status...")
	defer func() {
		c.mutex.Unlock()
		// DPrintf("checking task finished.")
	}()
	if c.done {
		return
	}
	done := true
	for index, taskInfo := range c.taskInfo {
		task := Task{
			TaskId:   index,
			TaskType: c.phase,
			NReduce:  c.nReduce,
			NMap:     len(c.files),
		}
		if task.TaskType == MapTask {
			task.InputFileName = c.files[index]
		}
		switch taskInfo.status {
		case Ready:
			done = false
			c.taskChannel = append(c.taskChannel, task)
			c.taskInfo[index].status = Queuing
		case Queuing:
			done = false
		case Running:
			done = false
			if time.Now().Sub(taskInfo.beginTime) > c.maxTaskRunTime {
                c.taskInfo[index].status = Queuing
				c.taskChannel = append(c.taskChannel, task)
			}
		case Finished:
		case Error:
			done = false
			c.taskInfo[index].status = Queuing
			c.taskChannel = append(c.taskChannel, task)
		default:
			panic("Task status error")
		}
	}
	if done {
		if c.phase == MapTask {
			DPrintf("Starting reduce tasks")
            c.taskChannel = c.taskChannel[:0]
			c.phase = ReduceTask
			c.taskInfo = make([]TaskInfo, c.nReduce)
		} else {
			c.done = true
		}
	}
}

// check Task status and put them into taskChannel if needed
func (c *Coordinator) checkTask() {
	for !c.Done() {
		go c.doCheckTask()
		time.Sleep(c.checkTaskInterval)
	}
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
	defer c.mutex.Unlock()
	c.mutex.Lock()
    /* if c.done {
        fmt.Printf("map task redo %d times\n", c.mapTaskRedoTime)
    } */
	return c.done
	//    return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mutex:             sync.Mutex{},
		nReduce:           nReduce,
		files:             files,
		taskChannel:       make([]Task, len(files)),
		phase:             MapTask,
		taskInfo:          make([]TaskInfo, len(files)),
		checkTaskInterval: time.Duration(time.Second * 3),
		maxTaskRunTime:    time.Duration(time.Second * 20),
		pullTaskInterval:  time.Duration(time.Millisecond * 500),
	}
    // fmt.Printf("nMap:%d, nReduce:%d", len(files), nReduce)
	go c.checkTask()
	c.server()
	DPrintf("Coordinator initialized successfully.")
	return &c
}
