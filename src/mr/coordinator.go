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
	Mutex       sync.Mutex
	Cond        *sync.Cond
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
	MTaskLen    int
	RtaskLen    int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for {
		if c.MapTasksDone() {
			break
		} else if task := c.fetchMapTask(); task != nil {
			reply.MapTask = task
			c.mapTaskStarted(task)
			return nil
		} else {
			c.Cond.Wait()
		}
	}

	for {
		if c.Done() {
			break
		} else if task := c.fetchReduceTask(); task != nil {
			reply.ReduceTask = task
			c.reduceTaskStarted(task)
			return nil

		} else {
			c.Cond.Wait()
		}
	}
	return nil

}

func (c *Coordinator) fetchMapTask() *MapTask {
	for _, task := range c.MapTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) fetchReduceTask() *ReduceTask {
	for _, task := range c.ReduceTasks {
		if task.State == WAITTING {
			return task
		}
	}
	return nil
}

func (c *Coordinator) mapTaskStarted(task *MapTask) {
	task.State = STARTED
	go func(task *MapTask) {
		timeOut := time.After(10 * time.Second)
		<-timeOut
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover map task %d \n MTaskLen: %d", task.Id, c.MTaskLen)
			task.State = WAITTING
			c.MTaskLen++
			log.Printf("MTaskLen: %d", c.MTaskLen)

			c.Cond.Broadcast()
		}
	}(task)
}

func (c *Coordinator) reduceTaskStarted(task *ReduceTask) {
	task.State = STARTED
	go func(task *ReduceTask) {
		timeOut := time.After(10 * time.Second)
		<-timeOut
		c.Mutex.Lock()
		defer c.Mutex.Unlock()
		if task.State != FINISHED {
			log.Printf("recover map task %d \n", task.Id)
			task.State = WAITTING
			c.RtaskLen++
			c.Cond.Broadcast()
		}
	}(task)
}

func (c *Coordinator) MapTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.MapTasks[args.TaskId].State = FINISHED
	c.MTaskLen--
	if c.MapTasksDone() {
		c.Cond.Broadcast()
	}
	return nil
}

func (c *Coordinator) ReduceTaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	c.ReduceTasks[args.TaskId].State = FINISHED
	c.RtaskLen--
	if c.Done() {
		c.Cond.Broadcast()
	}
	return nil
}

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
	return c.RtaskLen == 0
}

func (c *Coordinator) MapTasksDone() bool {
	return c.MTaskLen == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		RtaskLen: nReduce,
		MTaskLen: len(files),
	}

	// Your code here.
	c.Cond = sync.NewCond(&c.Mutex)
	c.MapTasks = make([]*MapTask, 0)
	for i, fileName := range files {
		task := &MapTask{
			Id:       i,
			FileName: fileName,
			NReduce:  nReduce,
			State:    WAITTING,
		}
		c.MapTasks = append(c.MapTasks, task)
	}

	for i := 0; i < nReduce; i++ {
		task := &ReduceTask{
			Id:    i,
			NMap:  len(files),
			State: WAITTING,
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
	}
	c.server()
	return &c
}
