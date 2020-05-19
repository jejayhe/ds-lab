package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type InProcessTask struct {
	Filename  string
	TaskId    int
	WorkerId  int
	StartTime *time.Time
}
type MapTaskInfo struct {
	Id         int
	Filename   string
	AssignedTo int
	Status     string // "idle" "processing" "finish"
	ProcessAt  *time.Time
}

type ReduceTaskInfo struct {
	Id        int
	Filenames []string
	Status    string
	ProcessAt *time.Time
}
type MapTask struct {
	Id       int
	Filename string
}
type Master struct {
	NextIndex int
	//MapTaskMap map[int]TaskInfo
	MapFileMap       map[int]*MapTaskInfo
	MapTasks         []int
	NReduce          int
	MapperInProcess  []*InProcessTask
	ReduceFileMap    map[int]*ReduceTaskInfo
	ReduceTasks      []int
	ReducerInProcess []*InProcessTask
	Phase            string // "map" "reduce"
	done             bool   // if true, all is done
	mutex            sync.Mutex
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

func (m *Master) GetNextIndex(req *GetNextIndexReq, resp *GetNextIndexResp) error {
	m.mutex.Lock()
	index := m.NextIndex
	m.NextIndex++
	m.mutex.Unlock()
	resp.Index = index
	resp.NReduce = m.NReduce
	return nil
}

func (m *Master) IsMapFinished() bool {
	for _, taskinfo := range m.MapFileMap {
		if taskinfo.Status != "finish" {
			return false
		}
	}
	m.done = true
	return true
}

func (m *Master) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	// assigns task to workerId
	m.mutex.Lock()
	if len(m.MapTasks) != 0 || !m.IsMapFinished() {
		// map phase
		now := time.Now()
		if len(m.MapTasks) != 0 {
			taskId := m.MapTasks[0]
			m.MapTasks = m.MapTasks[1:]
			taskinfo := m.MapFileMap[taskId]
			taskinfo.ProcessAt = &now
			taskinfo.AssignedTo = req.WorkerId
			taskinfo.Status = "processing"
			m.mutex.Unlock()
			resp.TaskId = taskId
			resp.File = m.MapFileMap[taskId].Filename
			resp.Type = "map"
			return nil
		} else {
			// waiting for some mapper to finish
			m.mutex.Unlock()
			return nil
		}
	}
	// for testing
	//m.mutex.Unlock()
	//resp.Done = true
	//if len(m.ReducerFiles) != 0 || len(m.ReducerInProcess) != 0 {
	//	// reduce phase
	//	if len(m.ReducerFiles) != 0 {
	//		file := m.ReducerFiles[0]
	//		m.ReducerFiles = m.ReducerFiles[1:]
	//		m.ReducerInProcess = append(m.ReducerInProcess, &InProcessTask{
	//			Filename:file,
	//			WorkerId: req.WorkerId,
	//			StartTime: time.Now(),
	//		})
	//		m.mutex.Unlock()
	//		resp.File = file
	//		resp.Type = "reduce"
	//		return nil
	//	} else {
	//		// waiting for some reducer to finish
	//		m.mutex.Unlock()
	//		return nil
	//	}
	//}
	m.mutex.Unlock()
	resp.Done = m.done
	return nil
}

func (m *Master) ReportTask(req *ReportTaskReq, resp *ReportTaskResp) error {
	if !req.Success {
		if req.Type == "map" {
			m.mutex.Lock()
			if m.MapFileMap[req.TaskId].Status != "finish" {
				m.MapTasks = append(m.MapTasks, req.TaskId)
				taskinfo := m.MapFileMap[req.TaskId]
				taskinfo.Status = "idle"
				taskinfo.AssignedTo = -1
				taskinfo.ProcessAt = nil
				m.mutex.Unlock()
				return nil
			}
			m.mutex.Unlock()
			return nil
		}
	} else {
		m.mutex.Lock()
		taskinfo := m.MapFileMap[req.TaskId]
		taskinfo.Status = "finish"
		m.mutex.Unlock()
		return nil
	}
	return nil
}

func (m *Master) CheckJobStatus() {
	for {
		time.Sleep(100 * time.Millisecond)
		m.mutex.Lock()
		// if a job took over 10 second,
		for i, taskinfo := range m.MapFileMap {
			if taskinfo.Status == "processing" {
				if taskinfo.ProcessAt == nil {
					log.Fatal("unknown situation")
				}
				if time.Now().Add(-5 * time.Second).After(*taskinfo.ProcessAt) {
					m.MapTasks = append(m.MapTasks, i)
					fmt.Printf("recycling map task: %d from worker %d \n", i, taskinfo.AssignedTo)
					taskinfo.ProcessAt = nil
					taskinfo.Status = "idle"
					taskinfo.AssignedTo = -1
				}
			}
		}
		m.mutex.Unlock()
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
	//ret := false

	// Your code here.
	ret := m.done
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		NextIndex:     0,
		NReduce:       nReduce,
		MapFileMap:    make(map[int]*MapTaskInfo),
		ReduceFileMap: make(map[int]*ReduceTaskInfo),
	}

	for i, file := range files {
		m.MapFileMap[i] = &MapTaskInfo{
			Id:         i,
			Filename:   file,
			AssignedTo: -1,
			Status:     "idle",
			ProcessAt:  nil,
		}
		m.MapTasks = append(m.MapTasks, i)
	}
	go m.CheckJobStatus()
	// Your code here.

	m.server()
	return &m
}
