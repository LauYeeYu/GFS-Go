package master

import "errors"

const (
	LockFileOrDirectory = iota
	LockFile
	LockDirectory
)

type LockTask struct {
	Name        string
	ReadOnly    bool
	Requirement int
	Subtasks    []*LockTask
}

func RequirementConflict(requirement1, requirement2 int) bool {
	if requirement1 == LockFileOrDirectory || requirement2 == LockFileOrDirectory {
		return false
	} else {
		return requirement1 != requirement2
	}
}

func MergeRequirement(requirement1, requirement2 int) (int, error) {
	if requirement1 == LockFileOrDirectory {
		return requirement2, nil
	} else if requirement2 == LockFileOrDirectory {
		return requirement1, nil
	} else if requirement1 == requirement2 {
		return requirement1, nil
	} else {
		return 0, errors.New("requirement conflict")
	}
}

func CanBeFile(requirement int) bool {
	return requirement == LockFile || requirement == LockFileOrDirectory
}

func CanBeDirectory(requirement int) bool {
	return requirement == LockDirectory || requirement == LockFileOrDirectory
}

func MergeReadOnly(readOnly1, readOnly2 bool) bool {
	return readOnly1 && readOnly2
}

// Insert inserts a lock task into the lock task tree. If the lock task already
// exists, the lock task tree is not modified.
// It returns the lock task that is inserted into the tree and a boolean value
// indicating whether the lock task already exists.
func (task *LockTask) Insert(lockTask *LockTask) (*LockTask, bool) {
	index := 0
	for index < len(task.Subtasks) && task.Subtasks[index].Name < lockTask.Name {
		index++
	}
	if index == len(task.Subtasks) {
		task.Subtasks = append(task.Subtasks, lockTask)
		return task.Subtasks[index], false
	} else if task.Subtasks[index].Name == lockTask.Name {
		return task.Subtasks[index], true
	} else {
		task.Subtasks = append(task.Subtasks[:index], append([]*LockTask{lockTask}, task.Subtasks[index:]...)...)
		return task.Subtasks[index], false
	}
}

func (task *LockTask) FindSubtask(name string) *LockTask {
	for _, subtask := range task.Subtasks {
		if subtask.Name == name {
			return subtask
		}
	}
	return nil
}

func makeLockTaskFromStrings(names []string, readOnly bool, requirement int) *LockTask {
	if len(names) == 0 {
		return nil
	}
	if len(names) == 1 {
		return &LockTask{
			Name:        names[0],
			ReadOnly:    readOnly,
			Requirement: requirement,
			Subtasks:    []*LockTask{},
		}
	}
	return &LockTask{
		Name:     names[0],
		ReadOnly: false,
		Subtasks: []*LockTask{makeLockTaskFromStrings(
			names[1:], readOnly, requirement,
		)},
	}
}

func MakeLockTaskFromStringSlice(names []string, readOnly bool, requirement int) *LockTask {
	if len(names) == 0 {
		return &LockTask{
			Name:     "",
			ReadOnly: readOnly,
			Subtasks: []*LockTask{},
		}
	}
	return &LockTask{
		Name:     "",
		Subtasks: []*LockTask{makeLockTaskFromStrings(names, readOnly, requirement)},
	}
}

// MergeLockTasks merges two lock tasks into one. If the two lock tasks have different
// names, an error is returned.
func MergeLockTasks(task1, task2 *LockTask) (*LockTask, error) {
	if task1 == nil {
		return task2, nil
	}
	if task2 == nil {
		return task1, nil
	}
	if task1.Name != task2.Name {
		return nil, errors.New("cannot merge lock tasks with different names")
	}
	requirement, err := MergeRequirement(task1.Requirement, task2.Requirement)
	if err != nil {
		return nil, err
	}
	newTask := &LockTask{
		Name:        task1.Name,
		ReadOnly:    MergeReadOnly(task1.ReadOnly, task2.ReadOnly),
		Requirement: requirement,
		Subtasks:    task1.Subtasks,
	}
	for _, subtask2 := range task2.Subtasks {
		subtask1 := task1.FindSubtask(subtask2.Name)
		if subtask1 == nil {
			_, _ = task1.Insert(subtask2)
		} else {
			_, err := MergeLockTasks(subtask1, subtask2)
			if err != nil {
				return nil, err
			}
		}
	}
	return newTask, nil
}
