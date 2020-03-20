package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
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
func NotifyFinish(taskId int, taskType TaskType) {
	args := NotifyArgs{}
	reply := NotifyReply{}
	args.TaskType = taskType
	args.TaskId = taskId

	for !call("Master.NotifyTask", &args, &reply) {
	}
	// log.Printf("Notify %v finish", args)
	// log.Println(args, reply)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	pid := os.Getpid()

	for {
		args := GetTaskArgs{fmt.Sprintf("%d", pid)}
		reply := GetTaskReply{}
		// log.Println("Request a task")
		for !call("Master.GetTask", &args, &reply) {
		}

		// log.Println(args, reply)
		taskType := reply.TaskType
		nReduce := reply.NReduce
		fileNames := reply.FileNames
		taskId := reply.TaskId
		switch taskType {
		case MAP_TASK:
			{
				var intermediates [][]KeyValue
				for i := 0; i < nReduce; i++ {
					intermediates = append(intermediates, []KeyValue{})
				}

				if len(fileNames) != 1 {
					log.Fatalf("No file for map")
					log.Fatalln(fileNames)
					continue
				}

				filename := fileNames[0]
				file, err := os.Open(filename)

				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}

				file.Close()
				kva := mapf(filename, string(content))
				// log.Printf("after map %v", kva[0])
				for _, v := range kva {
					i := ihash(v.Key) % nReduce
					intermediates[i] = append(intermediates[i], v)
				}

				for i := 0; i < nReduce; i++ {
					sort.Sort(ByKey(intermediates[i]))
					tmpFile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d", taskId, i))

					if err != nil {
						log.Fatal(err)
					}
					// defer os.Remove(tmpFile.Name()) // clean up
					for _, v := range intermediates[i] {
						if _, err := fmt.Fprintf(tmpFile, "%s %s\n", v.Key, v.Value); err != nil {
							log.Fatal(err)
						}
					}

					tmpFile.Close()
					err = os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%d-%d", taskId, i))

					if err != nil {
						log.Fatal(err)
					}
				}
				//notify finish
				NotifyFinish(taskId, MAP_TASK)
			}
		case REDUCE_TASK:
			{

				taskId := reply.TaskId
				// log.Printf("before start get reduce task")
				intermediateFiles := reply.FileNames
				kVal := make(map[string][]string)
				for _, fileName := range intermediateFiles {
					// log.Printf("proccess %v", fileName)
					file, err := os.Open(fileName)
					if err != nil {
						log.Fatal(fmt.Sprintf("open %s faild", fileName))
						continue
					}

					reader := bufio.NewReader(file)

					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							_ = file.Close()
							break
						}
						line = strings.Trim(line, "\n")
						pair := strings.Split(line, " ")
						kVal[pair[0]] = append(kVal[pair[0]], pair[1])
					}
					_ = file.Close()
				}
				tmpfile, err := ioutil.TempFile("", fmt.Sprintf("mr-out-%d", taskId))
				if err != nil {
					log.Fatal(err)
				}
				//do reduce task
				for key, values := range kVal {
					output := reducef(key, values)
					_, _ = fmt.Fprintf(tmpfile, "%v %v\n", key, output)
				}
				err = tmpfile.Close()
				if err != nil {
					log.Fatal(err)
				}
				err = os.Rename(tmpfile.Name(), fmt.Sprintf("mr-out-%d", taskId))
				if err != nil {
					log.Fatal(err)
				}
				// log.Printf(fmt.Sprintf("mr-out-%d", taskId))
				//notify finish
				NotifyFinish(taskId, REDUCE_TASK)
			}
		default:
			{
				log.Panicln("not task to do")
				time.Sleep(100)
			}

		}
	}

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

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// log.Printf("A new call %s", rpcname)
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
