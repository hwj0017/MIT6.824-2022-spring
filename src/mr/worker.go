package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	isStop := false
	// Your worker implementation here.
	for !isStop {
		task := getTask()
		switch task.TaskType {
		case ExitTask:
			{
				fmt.Println("All tasks are in progress, Worker exit")
				isStop = true
			}

		case MapTask:
			{
				taskReply := doMapTask(&task, mapf) // 执行map任务
				taskDone(&taskReply)
			}
		case ReduceTask:
			{
				taskReply := doReduceTask(&task, reducef)
				taskDone(&taskReply)
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

func getTask() Task {
	taskArgs := TaskArgs{}
	task := Task{}
	if ok := call("Coordinator.DistributeTask", &taskArgs, &task); ok {
		fmt.Printf("get task %d\n", task.TaskId)
	} else {
		fmt.Println("get task failed")
	}
	return task
}
func taskDone(taskReply *TaskReply) {
	reply := Task{}
	ok := call("Coordinator.ReplyTask", taskReply, &reply)
	if ok {
		fmt.Printf("Task Done!%d\n", taskReply.TaskId)
	}
}
func doMapTask(task *Task, mapf func(string, string) []KeyValue) TaskReply {
	reduceNum := task.ReduceNum
	hashKv := make([][]KeyValue, reduceNum)
	for _, fileName := range task.Files {
		// fmt.Println("map task: ", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()
		intermediate := mapf(fileName, string(content))
		for _, v := range intermediate {
			index := ihash(v.Key) % reduceNum
			hashKv[index] = append(hashKv[index], v) // 将该kv键值对放入对应的下标
		}
	}
	// 放入中间文件
	result := []string{}
	for i := 0; i < reduceNum; i++ {
		fileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		result = append(result, fileName)
		new_file, err := os.Create(fileName)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file) // 创建一个新的JSON编码器
		for _, kv := range hashKv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()
	}
	return TaskReply{TaskId: task.TaskId, TaskType: MapTask, Result: result}
}

func doReduceTask(task *Task, reducef func(string, []string) string) TaskReply {
	reduceNum := task.TaskId
	// fmt.Println("reduce task:", task.Files)
	intermediate := shuffle(task.Files)
	finalName := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(finalName)
	if err != nil {
		log.Fatal("create file failed:", err)
	}
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ { // i和j之间是一样的键值对，将一样的到一个values中
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	return TaskReply{TaskId: task.TaskId}
}

func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, fi := range files {
		file, err := os.Open(fi)
		if err != nil {
			log.Fatalf("cannot open %v", fi)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}
