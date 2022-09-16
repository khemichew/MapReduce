package mr

import (
	"fmt"
	"strconv"
	"testing"
)

var (
	dummyTasks = []Task{
		{
			Phase:         MapTask,
			InputFilepath: "input1",
			TaskId:        12,
			WorkerId:      2,
		},
		{
			Phase:         MapTask,
			InputFilepath: "input2",
			TaskId:        42,
			WorkerId:      21,
		},
	}

	taskTypes = map[Phase]string{
		MapTask:    "MapTask",
		ReduceTask: "ReduceTask",
		VoidTask:   "VoidTask",
		ExitTask:   "ExitTask",
	}

	taskStates = map[Progression]string{
		Idle:       "Idle",
		InProgress: "InProgress",
		Completed:  "Completed",
	}
)

// Generate tasks data structure with dummy tasks.
func setup() *Tasks {
	tasks := allocateTasks(len(dummyTasks))

	// Create dummy tasks
	for i := 0; i < len(dummyTasks); i++ {
		task := dummyTasks[i]
		tasks.Node[task.TaskId] = tasks.Queue[Idle].PushBack(&task)
		tasks.State[task.TaskId] = Idle
	}

	return tasks
}

func TestGenerateTasks(t *testing.T) {
	f1 := []string{"1", "2", "3", "4", "5"}
	f2 := []string{"1", "2", "3"}

	tests := []struct {
		files                []string
		nReduce              int
		wantTotalMapTasks    int
		wantTotalReduceTasks int
	}{
		{f1, 1, 5, 1},
		{f1, 3, 5, 3},
		{f2, 2, 3, 2},
	}

	// Test correct number of tasks generated
	for i, tc := range tests {
		mapTasks, reduceTasks := GenerateTasks(tc.files, tc.nReduce)

		// map tasks
		t.Run(fmt.Sprintf("map-%v", i), func(t *testing.T) {
			// Verify generated map tasks count
			if got := mapTasks.Queue[Idle].Len(); got != tc.wantTotalMapTasks {
				t.Fatalf("expected map tasks: %v, got %v", tc.wantTotalMapTasks, got)
			}
			if got := len(mapTasks.State); got != tc.wantTotalMapTasks {
				t.Fatalf("expected map tasks: %v, got %v", tc.wantTotalMapTasks, got)
			}
		})
		t.Run(fmt.Sprintf("reduce-%v", i), func(t *testing.T) {
			// Verify generated reduce tasks count
			if got := reduceTasks.Queue[Idle].Len(); got != tc.wantTotalReduceTasks {
				t.Fatalf("expected map tasks: %v, got %v", tc.wantTotalMapTasks, got)
			}
			if got := len(reduceTasks.State); got != tc.wantTotalReduceTasks {
				t.Fatalf("expected map tasks: %v, got %v", tc.wantTotalMapTasks, got)
			}
		})
	}
}

func TestFindTask(t *testing.T) {
	tasks := setup()

	tests := []struct {
		taskId int
		want   string
	}{
		{12, "input1"},
		{42, "input2"},
	}

	for i, tc := range tests {
		// Compare task names
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if task, ok := tasks.findTask(tc.taskId); !ok {
				t.Fatalf("expected: %v, but no tasks found", tc.want)
			} else if task.InputFilepath != tc.want {
				t.Fatalf("expected: %v, got: %v", tc.want, task.InputFilepath)
			}
		})
	}
}

func TestGetIdleTask(t *testing.T) {
	tests := map[string]struct {
		tasks *Tasks
		want  Phase
	}{
		"no-tasks":      {allocateTasks(0), VoidTask},
		"contain-tasks": {setup(), MapTask},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			for p := tc.tasks.Queue[Idle].Front(); p != nil; p = p.Next() {
				fmt.Printf("task: %v\n", taskTypes[p.Value.(*Task).Phase])
			}

			if got := tc.tasks.GetIdleTask().Phase; tc.want != got {
				t.Fatalf("expected: %v, got: %v", taskTypes[tc.want], taskTypes[got])
			}
		})
	}
}

func TestGetWorker(t *testing.T) {
	tasks := setup()

	tests := map[string]struct {
		taskId       int
		wantWorkerId int
	}{
		"task-does-not-exist": {-1, 0},
		"task-exists":         {12, 2},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			if got, _ := tasks.GetWorker(tc.taskId); got != tc.wantWorkerId {
				t.Fatalf("expected: %v, got: %v", tc.wantWorkerId, got)
			}
		})
	}
}

func TestSetWorker(t *testing.T) {
	tests := map[string]struct {
		tasks        *Tasks
		taskId       int
		newWorkerId  int
		wantWorkerId int
	}{
		"task-does-not-exist": {setup(), -1, -1, 0},
		"task-exists":         {setup(), 12, 6, 6},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			tc.tasks.SetWorker(tc.taskId, tc.newWorkerId)
			if got, _ := tc.tasks.GetWorker(tc.taskId); got != tc.wantWorkerId {
				t.Fatalf("expected: %v, got: %v", tc.wantWorkerId, got)
			}
		})
	}
}

func TestUpdateTaskStateDoesNothingIfTaskDoesNotExist(t *testing.T) {
	tests := map[string]struct {
		tasks    *Tasks
		taskId   int
		newState Progression
	}{
		"no-tasks":      {allocateTasks(0), 11, Completed},
		"contain-tasks": {setup(), 11, Idle},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			tc.tasks.UpdateTaskState(tc.taskId, tc.newState)
			if _, ok := tc.tasks.State[tc.taskId]; ok {
				t.Fatalf("did not expect tasks.State to contain non-existant task")
			}

			if _, ok := tc.tasks.Node[tc.taskId]; ok {
				t.Fatalf("did not expect tasks.Node to contain non-existant task")
			}

			// Search for entire queue
			l, _ := tc.tasks.Queue[tc.newState]
			for p := l.Front(); p != nil; p = p.Next() {
				if gotTaskId := p.Value.(*Task).TaskId; gotTaskId == tc.taskId {
					t.Fatalf("did not expect queue of new state to contain non-existant task")
				}
			}
		})
	}
}

func TestUpdateTaskWithExistingTasks(t *testing.T) {
	tests := map[string]struct {
		tasks    *Tasks
		taskId   int
		newState Progression
	}{
		"task-1": {setup(), 12, Completed},
		"task-2": {setup(), 42, Completed},
	}

	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			prevState := tc.tasks.State[tc.taskId]

			tc.tasks.UpdateTaskState(tc.taskId, tc.newState)

			// Verify task removed from previous state queue
			l := tc.tasks.Queue[prevState]
			for p := l.Front(); p != nil; p = p.Next() {
				if gotTaskId := p.Value.(*Task).TaskId; gotTaskId == tc.taskId {
					t.Fatalf("task was not removed from previous state queue")
				}
			}

			// Verify task inserted to new state queue
			l = tc.tasks.Queue[tc.newState]
			if l.Back().Value.(*Task).TaskId != tc.taskId {
				t.Fatalf("task was not inserted into new state queue")
			}

			// Check state of task
			if gotState := tc.tasks.State[tc.taskId]; gotState != tc.newState {
				t.Fatalf("expected: %v, got %v", taskStates[tc.newState], taskStates[gotState])
			}
		})
	}
}

func TestTasksDone(t *testing.T) {
	tasks := setup()
	checkNotDone := func(done bool) {
		if done {
			t.Fatalf("some tasks idle/in progress, but returned all tasks completed instead")
		}
	}
	checkDone := func(done bool) {
		if !done {
			t.Fatalf("all tasks completed but returned some tasks idle/in progress")
		}
	}

	checkNotDone(tasks.Done())
	tasks.UpdateTaskState(12, Completed)
	checkNotDone(tasks.Done())
	tasks.UpdateTaskState(42, Completed)
	checkDone(tasks.Done())
}
