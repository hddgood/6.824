package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

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
const (
	Map = iota
	Reduce
	Done
)

type Task struct {
	Id           int
	Type         int
	MapInputFile string
	WorkerId     int
	DeadLine     time.Time
}

func GetTaskId(t, idx int) string {
	return fmt.Sprintf("%d--%d", t, idx)
}

type ApplyForTaskArgs struct {
	WorkerId     int
	LastTaskId   int
	LastTaskType int
}

type ApplyForTaskReply struct {
	TaskId   int
	TaskType int
	FileName string
	NReduce  int
	NMap     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
