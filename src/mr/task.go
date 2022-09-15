package mr

import "container/list"

const (
	Idle = iota
	InProgress
	Completed
	TotalState
)

// Task is uniquely identified by TaskId.
// There are three types of tasks: MapTask, ReduceTask, and in
// the case of VoidTask and ExitTask, no other fields will be populated.
type Task struct {
	// One of MapTask, ReduceTask.
	Type int
	// Input filepath. Only applicable for map tasks.
	InputFilepath string
	// Unique identifier for each task.
	TaskId int
	// Identifies Worker executing the task.
	WorkerId int
}

// Tasks is a data structure designed to poll idle tasks efficiently,
// doubling as a tracker of tasks assigned to workers.
// Note: the data structure is *not* thread-safe. Use mutexes accordingly.
type Tasks struct {
	// [IdleQueue, InProgressQueue, CompletedQueue]
	Queue []*list.List
	// maps Task.TaskId to individual node containing Task
	Node map[int]*list.Element
	// maps Task.TaskId to one of Idle, InProgress, or Completed.
	State map[int]int
	// Number of total tasks. Constant once initialised.
	Capacity int
}

// Helper function to allocate memory used in a Tasks instance. Used in GenerateTasks.
func initialiseTasks(nMap, nReduce int) (mapTasks, reduceTasks *Tasks) {
	mapTasks = &Tasks{
		Queue:    make([]*list.List, TotalState),
		Node:     make(map[int]*list.Element),
		State:    make(map[int]int),
		Capacity: nMap}
	reduceTasks = &Tasks{
		Queue:    make([]*list.List, TotalState),
		Node:     make(map[int]*list.Element),
		State:    make(map[int]int),
		Capacity: nReduce}

	for i := 0; i < TotalState; i++ {
		mapTasks.Queue[i] = list.New()
	}

	for i := 0; i < TotalState; i++ {
		reduceTasks.Queue[i] = list.New()
	}

	return
}

// Create a list of map and reduce tasks, ready to be allocated by the coordinator.
func GenerateTasks(files []string, nReduce int) (mapTasks, reduceTasks *Tasks) {
	nMap := len(files)
	mapTasks, reduceTasks = initialiseTasks(nMap, nReduce)

	// Create map tasks
	for i, file := range files {
		task := &Task{Type: MapTask, InputFilepath: file, TaskId: i}
		mapTasks.Node[i] = mapTasks.Queue[Idle].PushBack(task)
		mapTasks.State[i] = Idle
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := &Task{Type: ReduceTask, TaskId: i}
		reduceTasks.Node[i] = reduceTasks.Queue[Idle].PushBack(task)
		reduceTasks.State[i] = Idle
	}

	return
}

// Return Task given Task.TaskId.
func (tasks *Tasks) findTask(taskId int) *Task {
	return tasks.Node[taskId].Value.(*Task)
}

// Insert task at the end of the queue, and update to latest state.
func (tasks *Tasks) insertTask(task *Task, state int) {
	tasks.Node[task.TaskId] = tasks.Queue[state].PushBack(task)
	tasks.State[task.TaskId] = state
}

// Remove task from queue.
func (tasks *Tasks) removeTask(task *Task) {
	state := tasks.State[task.TaskId]
	tasks.Queue[state].Remove(tasks.Node[task.TaskId])
}

// Assign an idle task if available, otherwise return a void task
func (tasks *Tasks) GetIdleTask() *Task {
	idleTasks := tasks.Queue[Idle]
	if idleTasks.Len() > 0 {
		task := idleTasks.Front().Value.(*Task)
		tasks.removeTask(task)
		tasks.insertTask(task, InProgress)
		return task
	}
	return &Task{Type: VoidTask}
}

// UpdateTask updates the state of the task to Idle, InProgress, or Completed.
func (tasks *Tasks) UpdateTask(taskId int, newState int) {
	task := tasks.findTask(taskId)
	tasks.removeTask(task)
	tasks.insertTask(task, newState)
}

func (tasks *Tasks) GetWorker(taskId int) int {
	return tasks.findTask(taskId).WorkerId
}

func (tasks *Tasks) SetWorker(taskId int, workerId int) {
	tasks.findTask(taskId).WorkerId = workerId
}

// Return status of phase completion.
func (tasks *Tasks) Done() bool {
	return tasks.Queue[Completed].Len() == tasks.Capacity
}
