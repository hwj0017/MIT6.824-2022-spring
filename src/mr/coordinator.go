package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskState int

const (
	Waiting TaskState = iota
	Running
	Done
)

type TaskMetaInfo struct {
	taskAddr  *Task // 任务指针
	state     TaskState
	startTime time.Time
}

type TaskMetaHolder struct {
	metaMap   map[int]*TaskMetaInfo
	doneCount int
}

func (t *TaskMetaHolder) addTask(task *Task) {
	t.metaMap[task.TaskId] = &TaskMetaInfo{task, Waiting, time.Now()}
}

func (t *TaskMetaHolder) runTask(taskId int) {
	t.metaMap[taskId].state = Running
	t.metaMap[taskId].startTime = time.Now()
}

func (t *TaskMetaHolder) doneTask(taskId int) bool {
	if value, e := t.metaMap[taskId]; e && value.state != Done {
		value.state = Done
		t.doneCount++
		return true
	}
	return false
}

type State int

const (
	InitState State = iota
	MapState
	ReduceState
	DoneState
)

type Coordinator struct {
	// Your definitions here.
	holderMu       sync.Mutex
	stateMu        sync.Mutex
	reduceFilesMu  sync.Mutex
	taskChan       chan *Task
	mapFiles       []string
	mapNum         int
	reduceFiles    [][]string
	reduceNum      int
	nextTaskId     int
	taskMetaHolder TaskMetaHolder
	state          State
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(time.Second)
		c.stateMu.Lock()
		if c.state == DoneState {
			c.stateMu.Unlock()
			break
		}
		c.stateMu.Unlock()
		c.holderMu.Lock()
		for _, task := range c.taskMetaHolder.metaMap {
			if task.state == Running && time.Since(task.startTime) > time.Second*10 {
				fmt.Printf("task timeout%d\n", task.taskAddr.TaskId)
				task.state = Waiting
				c.taskChan <- task.taskAddr
			}
		}
		c.holderMu.Unlock()
	}
}
func (c *Coordinator) makeMapTask() {
	tasks := []*Task{}
	for i := 0; i < c.mapNum; i++ {
		tasks = append(tasks, &Task{TaskId: c.nextTaskId, TaskType: MapTask, Files: []string{c.mapFiles[i]}, ReduceNum: c.reduceNum})
		c.nextTaskId++
	}
	c.holderMu.Lock()
	for _, task := range tasks {
		c.taskMetaHolder.addTask(task)
	}
	c.holderMu.Unlock()
	for _, task := range tasks {
		c.taskChan <- task
	}

}

func (c *Coordinator) makeReduceTask() {

	tasks := []*Task{}
	for i := 0; i < c.reduceNum; i++ {
		tasks = append(tasks, &Task{TaskId: c.nextTaskId, TaskType: ReduceTask, Files: c.reduceFiles[i], ReduceNum: c.reduceNum})
		c.nextTaskId++
	}
	c.holderMu.Lock()
	for _, task := range tasks {
		c.taskMetaHolder.addTask(task)
	}
	c.holderMu.Unlock()
	for _, task := range tasks {
		c.taskChan <- task
	}

}

func (c *Coordinator) shutdown() {
	close(c.taskChan)
}
func (c *Coordinator) DistributeTask(args *TaskArgs, reply *Task) error {
	for {
		task, ok := <-c.taskChan
		if !ok {
			*reply = Task{TaskType: ExitTask}
			break
		}
		c.holderMu.Lock()
		if c.taskMetaHolder.metaMap[task.TaskId].state == Waiting {
			*reply = *task
			c.taskMetaHolder.runTask(reply.TaskId)
			c.holderMu.Unlock()
			break
		}
		c.holderMu.Unlock()
	}
	return nil
}

func (c *Coordinator) ReplyTask(taskReply *TaskReply, reply *Task) error {
	c.holderMu.Lock()
	res := c.taskMetaHolder.doneTask(taskReply.TaskId)
	count := c.taskMetaHolder.doneCount
	c.holderMu.Unlock()
	if res {
		if taskReply.TaskType == MapTask {
			c.reduceFilesMu.Lock()
			for _, file := range taskReply.Result {
				parts := strings.Split(file, "-")
				suffix := parts[len(parts)-1]
				num, err := strconv.Atoi(suffix)
				if err != nil {
					fmt.Println("转换失败:", err)
					continue
				}
				if num >= c.reduceNum {
					fmt.Println("reduceNum:", c.reduceNum, "num:", num)
					continue
				}
				c.reduceFiles[num] = append(c.reduceFiles[num], file)
			}
			c.reduceFilesMu.Unlock()
		}
		c.stateMu.Lock()
		if count == c.mapNum || count == c.mapNum+c.reduceNum {
			c.toNextState()
		}
		c.stateMu.Unlock()
	}
	return nil
}

func (c *Coordinator) toNextState() {
	switch c.state {
	case InitState:
		c.state = MapState
		go c.makeMapTask()
	case MapState:
		c.state = ReduceState
		go c.makeReduceTask()
	case ReduceState:
		c.state = DoneState
		c.shutdown()
	}
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
	c.stateMu.Lock()
	c.toNextState()
	c.stateMu.Unlock()
	go c.checkTimeout()
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	// ret := false
	// Your code here.
	return c.state == DoneState
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c = Coordinator{taskChan: make(chan *Task, max(len(files), nReduce)), mapFiles: files, reduceFiles: make([][]string, nReduce), mapNum: len(files), reduceNum: nReduce, nextTaskId: 0, taskMetaHolder: TaskMetaHolder{metaMap: make(map[int]*TaskMetaInfo), doneCount: 0}, state: InitState}
	c.server()
	return &c
}
