package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Lock          sync.Mutex
	Status        int
	NMap          int
	NReduce       int
	Tasks         map[string]Task
	AvailableTask chan Task
}

// Your code here -- RPC handlers for the worker to call.

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
	c.Lock.Lock()
	ret = c.Status == Done
	c.Lock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Status:        Map,
		NMap:          len(files),
		NReduce:       nReduce,
		Tasks:         map[string]Task{},
		AvailableTask: make(chan Task, max(len(files), nReduce)),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			Id:           i,
			Type:         Map,
			MapInputFile: file,
			WorkerId:     -1,
		}
		c.Tasks[GetTaskId(task.Type, task.Id)] = task
		c.AvailableTask <- task
	}

	log.Printf("Coordinator start\n")

	c.server()

	// task检测是否出现问题并回收
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			for _, task := range c.Tasks {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					log.Printf("Found timed-out map task %d previously running on worker %s. Prepare to re-assign", task.Id, task.WorkerId)
					task.WorkerId = -1
					c.AvailableTask <- task
				}
			}
		}
	}()

	return &c
}

func (c *Coordinator) ApplyForTask(arg *ApplyForTaskArgs, rep *ApplyForTaskReply) error {
	// 对上个任务的完成情况进行一个判断
	if arg.LastTaskType != -1 {
		taskId := GetTaskId(arg.LastTaskType, arg.LastTaskId)
		c.Lock.Lock()
		if task, ok := c.Tasks[taskId]; ok && task.WorkerId == arg.WorkerId {
			log.Printf("Mark task %d as finished on worker %s\n", task.Type, task.Id, arg.WorkerId)
			// 将临时文件变为最终提交文件
			if c.Status == Map {
				for i := 0; i < c.NReduce; i++ {
					err := os.Rename(tmpmapworkfile(arg.WorkerId, arg.LastTaskId, i), finalmapfile(arg.LastTaskId, i))
					if err != nil {
						log.Fatalf("Failed to mark map output file `%s` as final: %e", tmpmapworkfile(arg.WorkerId, arg.LastTaskId, i), err)
					}
				}
			} else if c.Status == Reduce {
				for i := 0; i < c.NReduce; i++ {
					err := os.Rename(tmpreduceworkfile(arg.WorkerId, arg.LastTaskId), finalreducefile(i))
					if err != nil {
						log.Fatalf("Failed to mark reduce output file `%s` as final: %e", tmpreduceworkfile(arg.WorkerId, arg.LastTaskId), err)
					}
				}
			}
			delete(c.Tasks, taskId)
			if len(c.Tasks) == 0 {
				c.changeStatus()
			}
		}
		c.Lock.Unlock()
	}

	task, ok := <-c.AvailableTask
	// 任务完成
	if !ok {
		return nil
	}

	// 分配任务
	c.Lock.Lock()
	defer c.Lock.Unlock()
	log.Printf("Assign task %d to worker %dls", task.Id, arg.WorkerId)
	rep.TaskId = task.Id
	rep.TaskType = task.Type
	rep.NMap = c.NMap
	rep.NReduce = c.NReduce
	rep.FileName = task.MapInputFile
	task.WorkerId = arg.WorkerId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.Tasks[GetTaskId(task.Type, task.Id)] = task
	return nil
}

func (c *Coordinator) changeStatus() {
	if c.Status == Map {
		c.Status = Reduce
		log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")

		// 生成 Reduce Task
		for i := 0; i < c.NReduce; i++ {
			task := Task{
				Type: Reduce,
				Id:   i,
			}
			c.Tasks[GetTaskId(task.Type, task.Id)] = task
			c.AvailableTask <- task
		}

	} else if c.Status == Reduce {
		c.Status = Done
		log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.AvailableTask)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func tmpmapworkfile(workerId, taskId, reduceId int) string {
	return fmt.Sprintf("tmp-map-file-%d-%d-%d", workerId, taskId, reduceId)
}

func finalmapfile(taskId, reduceId int) string {
	return fmt.Sprintf("final-map-file-%d-%d", taskId, reduceId)
}

func tmpreduceworkfile(workerId, taskId int) string {
	return fmt.Sprintf("tmp-reduce-file-%d-%d-%d", workerId, taskId)
}

func finalreducefile(reduceId int) string {
	return fmt.Sprintf("final-reduce-file-%d", reduceId)
}
