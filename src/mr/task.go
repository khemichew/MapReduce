package mr

import "container/list"

const (
	Idle = iota
	InProgress
	Completed
	TotalState
)

// Task is uniquely identified by Id.
// There are three types of tasks: MapTask, ReduceTask, and in
// the case of VoidTask and ExitTask, no other fields will be populated.
type Task struct {
	Type          TaskType
	Input         string // only applicable for map tasks
	Id            int
	NumMapTask    int
	NumReduceTask int
}

// Tasks is a data structure designed to poll idle tasks efficiently.
// Note: the data structure is *not* thread-safe. Use mutexes accordingly.
type Tasks struct {
	Queue    []*list.List // [IdleQueue, InProgressQueue, CompletedQueue]
	Map      map[*Task]int
	capacity int
}

// Helper function to allocate memory used in a Tasks instance. Used in generateTasks.
func initialiseTasks(nMap, nReduce int) (mapTasks, reduceTasks *Tasks) {
	mapTasks = &Tasks{Queue: make([]*list.List, TotalState), Map: make(map[*Task]int), capacity: nMap}
	reduceTasks = &Tasks{Queue: make([]*list.List, TotalState), Map: make(map[*Task]int), capacity: nReduce}

	for i := 0; i < TotalState; i++ {
		mapTasks.Queue[i] = list.New()
	}

	for i := 0; i < TotalState; i++ {
		reduceTasks.Queue[i] = list.New()
	}

	return
}

// Create a list of map and reduce tasks, ready to be allocated by the coordinator.
func generateTasks(files []string, nReduce int) (mapTasks, reduceTasks *Tasks) {
	nMap := len(files)
	mapTasks, reduceTasks = initialiseTasks(nMap, nReduce)

	// Create map tasks
	for i, file := range files {
		task := &Task{Type: MapTask, Input: file, Id: i, NumMapTask: nMap, NumReduceTask: nReduce}
		mapTasks.Queue[Idle].PushBack(task)
		mapTasks.Map[task] = Idle
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := &Task{Type: ReduceTask, Input: "", Id: i, NumMapTask: nMap, NumReduceTask: nReduce}
		reduceTasks.Queue[Idle].PushBack(task)
		reduceTasks.Map[task] = Idle
	}

	return
}

// Assign an idle task if available, otherwise return a void task
func (tasks *Tasks) getTask() *Task {
	idleTasks := tasks.Queue[Idle]
	if idleTasks.Len() > 0 {
		// Remove from idleQueue and append to inProgressQueue
		task := idleTasks.Remove(idleTasks.Front()).(*Task)
		tasks.Queue[InProgress].PushBack(task)

		// Update state of task in hashmap
		tasks.Map[task] = InProgress
		return task
	}
	return &Task{Type: VoidTask}
}

// Return status of phase completion.
func (tasks *Tasks) done() bool {
	return tasks.Queue[Completed].Len() == tasks.capacity
}
