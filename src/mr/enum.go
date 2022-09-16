package mr

type Phase int
type Progression int

const TotalState = 3

const (
	MapTask Phase = iota
	ReduceTask
	VoidTask
	ExitTask
)

const (
	Idle Progression = iota
	InProgress
	Completed
)
