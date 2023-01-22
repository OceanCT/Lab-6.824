package mr

import (
	"fmt"
	"log"
)

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
)

const Debug = false

func DPrintf(format string, value ...interface{}) {
	if Debug {
		log.Printf(format, value...)
	}
}

type Task struct {
	TaskId        int
	TaskType      TaskType
	InputFileName string
	NReduce       int
	NMap          int
}

func getReduceName(mapTaskId, reduceTaskId int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskId, reduceTaskId)
}

func getResultName(reduceTaskId int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskId)
}
