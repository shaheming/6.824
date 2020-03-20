package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	fileName  string
	state     int //0 idl 1 in process 2 finished
	startTime *time.Time
	endTime   *time.Time
	taskId    int
}
type reduceTask struct {
	fileNames []string
	state     int //0 idl 1 in process 2 finished
	startTime *time.Time
	endTime   *time.Time
	taskId    int
}
type mapTasks struct {
	taskList       []mapTask
	idleTasks      []*mapTask
	inProcessTasks []*mapTask
	doneTasks      []*mapTask
}
type reduceTasks struct {
	taskList       []reduceTask
	idleTasks      []*reduceTask
	inProcessTasks []*reduceTask
	doneTasks      []*reduceTask
}
type Master_State int32

const (
	MAP_STATE    Master_State = 1
	REDUCE_STATE Master_State = 2
	DONE_STATE   Master_State = 3
)

type Master struct {
	// Your definitions here.
	state   Master_State //0 map 1 reduce 2 finished
	mT      mapTasks
	rT      reduceTasks
	mu      sync.Mutex
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) DesignateTask(args *Args, reply *Reply) error {
	reply.Y = args.X + 1
	return nil
}
func (m *Master) GetMapTask() (string, int) {
	t := time.Now()
	//find a stalled task
	for _, task := range m.mT.inProcessTasks {
		if (t.Sub(*task.startTime)).Seconds() > 10 {
			*task.startTime = time.Now()
			return task.fileName, task.taskId
		}
	}

	if len(m.mT.idleTasks) > 0 {
		// log.Printf("idle map tasks %v", m.mT.idleTasks)
		task := m.mT.idleTasks[0]
		m.mT.idleTasks = m.mT.idleTasks[1:]
		newTime := new(time.Time)
		*newTime = time.Now()
		task.startTime = newTime
		m.mT.inProcessTasks = append(m.mT.inProcessTasks, task)
		task.state = 1
		// log.Printf("idle map tasks %v", m.mT.idleTasks)
		return task.fileName, task.taskId
	} else {
		return "", -1
	}

}
func (m *Master) GetReduceTask() ([]string, int) {
	t := time.Now()
	for _, task := range m.rT.inProcessTasks {
		// log.Println(*task)
		if (t.Sub(*task.startTime)).Seconds() > 10 {
			*task.startTime = time.Now()
			return task.fileNames, task.taskId
		}
	}
	//log.Println("After scan reduce task")
	if len(m.rT.idleTasks) > 0 {
		task := m.rT.idleTasks[0]
		newTime := new(time.Time)
		*newTime = time.Now()
		task.startTime = newTime
		// log.Printf("m.rT.idleTasks %v", m.rT.idleTasks)
		// log.Println(*task)
		m.rT.inProcessTasks = append(m.rT.inProcessTasks, task)
		// log.Printf("m.rT.inProcessTasks %v", m.rT.inProcessTasks)
		task.state = 1
		//log.Println(task.fileNames)
		m.rT.idleTasks = m.rT.idleTasks[1:]
		return task.fileNames, task.taskId
	} else {
		return make([]string, 0), -1
	}
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.state {
	case MAP_STATE:
		{
			//get a map task
			fileName, taskId := m.GetMapTask()
			if fileName != "" {
				reply.FileNames = append(reply.FileNames, fileName)
				reply.TaskId = taskId
				reply.TaskType = MAP_TASK
				reply.NReduce = m.nReduce
			}
			return nil
		}
	case REDUCE_STATE:
		{
			//asign reduce task
			//log.Println("Get reduce task")
			fileNames, taskId := m.GetReduceTask()
			reply.FileNames = fileNames
			reply.TaskId = taskId
			reply.TaskType = REDUCE_TASK
			reply.NReduce = m.nReduce
			return nil
		}
	case DONE_STATE:
		return nil

	}
	return nil
}
func (m *Master) NotifyMapTask(args *NotifyArgs) error {
	for i := 0; i < len(m.mT.inProcessTasks); i++ {
		if m.mT.inProcessTasks[i].taskId == args.TaskId {
			task := m.mT.inProcessTasks[i]
			endTime := new(time.Time)
			*endTime = time.Now()
			task.endTime = endTime
			task.state = 2
			m.mT.doneTasks = append(m.mT.doneTasks, task)
			//log.Println(m.mT.inProcessTasks)
			m.mT.inProcessTasks = append(m.mT.inProcessTasks[:i], m.mT.inProcessTasks[i+1:]...)
			//log.Println(m.mT.inProcessTasks)
			//log.Println("Add new reduce task")
			for id := 0; id < m.nReduce; id++ {
				m.rT.taskList[id].fileNames = append(m.rT.taskList[id].fileNames, fmt.Sprintf("mr-%d-%d", task.taskId, id))
				// log.Printf("m.rT.taskList[id].fileNames %v", m.rT.taskList[id].fileNames)
			}
			break
		}
	}
	if len(m.mT.doneTasks) == len(m.mT.taskList) {
		for i := range m.rT.taskList {
			// log.Printf("Print rT tasks%v", task)
			m.rT.idleTasks = append(m.rT.idleTasks, &m.rT.taskList[i])
			// log.Printf("idle tasks filenames %v", m.rT.idleTasks[i].fileNames)
			// log.Printf("taskList tasks filenames %v", m.rT.taskList[i].fileNames)
		}
		// log.Printf("idle tasks %v", m.rT.idleTasks)
		m.state = REDUCE_STATE
	}
	return nil
}
func (m *Master) NotifyReduceTask(args *NotifyArgs) error {
	var i = 0
	for ; i < len(m.rT.inProcessTasks); i++ {
		if m.rT.inProcessTasks[i].taskId == args.TaskId {
			task := m.rT.inProcessTasks[i]
			endTime := new(time.Time)
			*endTime = time.Now()
			task.endTime = endTime
			task.state = 2
			m.rT.doneTasks = append(m.rT.doneTasks, task)
			_ = copy(m.rT.inProcessTasks[i:], m.rT.inProcessTasks[i+1:])
			// log.Printf("inproccessTask: %v ", m.rT.inProcessTasks)
			m.rT.inProcessTasks = m.rT.inProcessTasks[:len(m.rT.inProcessTasks)-1]
			break
		}
	}
	if len(m.rT.doneTasks) == len(m.rT.taskList) {

		m.state = DONE_STATE
	}
	return nil
}

func (m *Master) NotifyTask(args *NotifyArgs, reply *NotifyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case MAP_TASK:
		{
			//log.Println("Notify map task")
			return m.NotifyMapTask(args)
		}
	case REDUCE_TASK:
		{
			//log.Println("Notify reduce task")
			return m.NotifyReduceTask(args)
		}
	default:
		return errors.New("Wrong type")

	}

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := false
	if m.state == DONE_STATE {
		ret = true
	}
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	if len(files) == 0 {
		m.state = DONE_STATE
	} else {
		m.nReduce = nReduce
		m.state = MAP_STATE
		//init the task
		for i, file := range files {
			m.mT.taskList = append(m.mT.taskList, mapTask{file, 0, nil, nil, i})
			m.mT.idleTasks = append(m.mT.idleTasks, &m.mT.taskList[i])
			// log.Printf("%v", m.mT.idleTasks)
		}
		for i := 0; i < nReduce; i++ {
			m.rT.taskList = append(m.rT.taskList, reduceTask{make([]string, 0), 0, nil, nil, i})
		}
	}

	// Your code here.

	m.server()
	return &m
}
