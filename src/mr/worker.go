package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := os.Getpid()

	lastTaskId := -1
	lastTaskType := -1

	for {
		arg := &ApplyForTaskArgs{
			WorkerId:     workerId,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		reply := &ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", arg, reply)
		if reply == nil {
			goto End
		}
		if reply.TaskType == Map {
			doMapTask(reply.TaskId, workerId, reply.FileName, reply.NReduce, mapf)
		} else if reply.TaskType == Reduce {
			doReduceTask(reply.TaskId, workerId, reply.NMap, reducef)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
End:
}

func doMapTask(taskId int, workerId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Printf("open file %s fail", fileName)
		return
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("read file %s fail", fileName)
		return
	}

	kv := mapf(fileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, val := range kv {
		hashed := ihash(val.Key) % nReduce
		hashedKva[hashed] = append(hashedKva[hashed], val)
	}

	for i := 0; i < nReduce; i++ {
		outFile, _ := os.Create(tmpmapworkfile(workerId, taskId, i))
		for _, v := range hashedKva[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", v.Key, v.Value)
		}
		outFile.Close()
	}
}

func doReduceTask(taskId int, workerId int, nMap int, reducef func(string, []string) string) {
	var lines []string
	for i := 0; i < nMap; i++ {
		fileName := finalmapfile(taskId, workerId)
		file, err := os.Open(fileName)
		defer file.Close()
		if err != nil {
			log.Printf("open file %s fail", fileName)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("?????? %s ???????????????", fileName)
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}
	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   parts[0],
			Value: parts[1],
		})
	}

	// ??? Key ???????????????????????????
	sort.Sort(Bykey())

	ofile, _ := os.Create(tmpreduceworkfile(workerId, taskId))

	// ??? Key ?????????????????? Value ???????????????????????? Reduce ??????
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// ?????????????????????
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
