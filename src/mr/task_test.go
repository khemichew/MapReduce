package mr

import (
	"container/list"
	"fmt"
	"strconv"
	"testing"
)

var (
	dummyTasks = []Task{
		{
			Type:          MapTask,
			InputFilepath: "input1",
			TaskId:        12,
			WorkerId:      2,
		},
		{
			Type:          MapTask,
			InputFilepath: "input2",
			TaskId:        42,
			WorkerId:      21,
		},
	}
)

// Generate tasks data structure with dummy tasks.
func setup() *Tasks {
	tasks := Tasks{
		Queue:    make([]*list.List, TotalState),
		Node:     make(map[int]*list.Element),
		State:    make(map[int]int),
		Capacity: len(dummyTasks)}

	for i := 0; i < TotalState; i++ {
		tasks.Queue[i] = list.New()
	}

	// Create dummy tasks
	for i := 0; i < len(dummyTasks); i++ {
		task := dummyTasks[i]
		tasks.Node[task.TaskId] = tasks.Queue[Idle].PushBack(&task)
		tasks.State[task.TaskId] = Idle
	}

	return &tasks
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
			if task := tasks.findTask(tc.taskId); task == nil {
				t.Fatalf("expected: %v, got: nil", tc.want)
			} else if task.InputFilepath != tc.want {
				t.Fatalf("expected: %v, got: %v", tc.want, task.InputFilepath)
			}
		})
	}
}
