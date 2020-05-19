package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type InProcessTask struct {
	Filename    string
	WorkerIndex int
	StartTime   time.Time
}
type Master struct {
	NextIndex        int
	MapperFiles      []string
	NReduce          int
	MapperInProcess  []InProcessTask
	ReducerFiles     []string
	ReducerInProcess []InProcessTask
	Phase            string // "map" "reduce"
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

func (m *Master) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	// assigns task to workerId
	m.mutex.Lock()
	if len(m.MapperFiles) != 0 || len(m.MapperInProcess) != 0 {
		// map phase
		if len(m.MapperFiles) != 0 {
			file := m.MapperFiles[0]
			m.MapperFiles = m.MapperFiles[1:]
			m.mutex.Unlock()
			resp.File = file
			resp.Type = "map"
			return nil
		} else {
			// waiting for some mapper to finish
			m.mutex.Unlock()
			return nil
		}
	}
	// for testing
	m.mutex.Unlock()
	resp.Done = true
	if len(m.ReducerFiles) != 0 || len(m.ReducerInProcess) != 0 {
		// reduce phase
		if len(m.ReducerFiles) != 0 {
			file := m.ReducerFiles[0]
			m.ReducerFiles = m.ReducerFiles[1:]
			m.mutex.Unlock()
			resp.File = file
			resp.Type = "reduce"
			return nil
		} else {
			// waiting for some reducer to finish
			m.mutex.Unlock()
			return nil
		}
	}
	m.mutex.Unlock()
	resp.Done = true
	return nil
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		NextIndex:   0,
		MapperFiles: files,
		NReduce:     nReduce,
	}

	// Your code here.

	m.server()
	return &m
}
