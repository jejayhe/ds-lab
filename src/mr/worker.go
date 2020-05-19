package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var WorkerId int
var NReduce int

//
// main/mrworker.go calls this function.
//
var Mapf func(string, string) []KeyValue
var Reducef func(string, []string) string

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	Mapf = mapf
	Reducef = reducef
	// uncomment to send the Example RPC to the master.
	//CallExample()
	GetNextIndex()
	//GetTask()
	Cycle()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func GetNextIndex() {
	req := GetNextIndexReq{}
	resp := GetNextIndexResp{}
	call("Master.GetNextIndex", &req, &resp)
	WorkerId = resp.Index
	fmt.Printf("worker %d starts-------\n", WorkerId)
	NReduce = resp.NReduce
}

func GetTask() *GetTaskResp {
	req := GetTaskReq{WorkerId: WorkerId}
	resp := GetTaskResp{}
	call("Master.GetTask", &req, &resp)
	fmt.Printf("worker %d receives %s task: %s %s status: %v \n", WorkerId, resp.Type, resp.File, resp.Files, resp.Done)
	return &resp
}

func ReportTask(Type string, taskId int, success bool) {
	req := ReportTaskReq{
		Type:    Type,
		TaskId:  taskId,
		Success: success,
	}
	resp := ReportTaskResp{}
	call("Master.ReportTask", &req, &resp)
}

func Cycle() {
	for {
		resp := GetTask()
		if resp.Done {
			break
		}
		if resp.Type == "map" && resp.File == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.Type == "reduce" && resp.Files == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		switch resp.Type {
		case "map":
			err := DoMap(resp.File, resp.TaskId)
			if err != nil {
				ReportTask("map", resp.TaskId, false)
			} else {
				ReportTask("map", resp.TaskId, true)
			}
		case "reduce":
			err := DoReduce(resp.Files, resp.TaskId)
			if err != nil {
				ReportTask("reduce", resp.TaskId, false)
			} else {
				ReportTask("reduce", resp.TaskId, true)
			}
		}

	}
}
func DoMap(filename string, mapTaskId int) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := Mapf(filename, string(content))
	immediate_file_names := make([]string, 0)
	tfds := make([]*os.File, 0)
	encs := make([]*json.Encoder, 0)
	for i := 0; i < NReduce; i++ {
		immediate_file_name := fmt.Sprintf("mr-%d-%d", mapTaskId, i)
		immediate_file_names = append(immediate_file_names, immediate_file_name)
		//fd, err:=os.Create(immediate_file_name)
		tfd, err := ioutil.TempFile(".", "")
		if err != nil {
			log.Fatalf("cannot open temp file")
		}
		tfds = append(tfds, tfd)
		enc := json.NewEncoder(tfd)
		encs = append(encs, enc)
	}
	for _, kv := range kva {
		encs[ihash(kv.Key)%NReduce].Encode(&kv)
	}
	for _, fd := range tfds {
		fd.Close()
	}
	for i, tfd := range tfds {
		err := os.Rename(tfd.Name(), immediate_file_names[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func DoReduce(filenames []string, reduceTaskId int) error {
	kva := make([]KeyValue, 0)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	intermediate := kva
	sort.Sort(ByKey(intermediate))
	tfd, err := ioutil.TempFile(".", "")
	if err != nil {
		log.Fatalf("cannot open temp file")
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := Reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tfd, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tfd.Close()
	err = os.Rename(tfd.Name(), fmt.Sprintf("mr-out-%d", reduceTaskId))
	if err != nil {
		return err
	}
	return nil
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	//fmt.Printf("sockname is: %v \n", sockname)
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
