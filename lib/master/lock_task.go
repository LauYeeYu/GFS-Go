package master

import "errors"

type LockTask struct {
	Name     string
	readOnly bool
	Subtasks []*LockTask
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

func makeLockTaskFromStrings(names []string, readOnly bool) *LockTask {
	if len(names) == 0 {
		return nil
	}
	if len(names) == 1 {
		return &LockTask{Name: names[0], readOnly: readOnly}
	}
	return &LockTask{
		Name:     names[0],
		readOnly: false,
		Subtasks: []*LockTask{makeLockTaskFromStrings(names[1:], readOnly)},
	}
}

func MakeLockTaskFromStringSlice(names []string, readOnly bool) *LockTask {
	if len(names) == 0 {
		return &LockTask{
			Name:     "",
			readOnly: readOnly,
			Subtasks: []*LockTask{},
		}
	}
	return &LockTask{
		Name:     "",
		Subtasks: []*LockTask{makeLockTaskFromStrings(names, readOnly)},
	}
}

// Merge merges two lock tasks into one. If the two lock tasks have different
// names, an error is returned.
func Merge(task1, task2 *LockTask) (*LockTask, error) {
	if task1 == nil {
		return task2, nil
	}
	if task2 == nil {
		return task1, nil
	}
	if task1.Name != task2.Name {
		return nil, errors.New("cannot merge lock tasks with different names")
	}
	if len(task1.Subtasks) == 0 {
		return task2, nil
	}
	if len(task2.Subtasks) == 0 {
		return task1, nil
	}
	for _, subtask2 := range task2.Subtasks {
		subtask1 := task1.FindSubtask(subtask2.Name)
		if subtask1 == nil {
			_, _ = task1.Insert(subtask2)
		} else {
			_, err := Merge(subtask1, subtask2)
			if err != nil {
				return nil, err
			}
		}
	}
	return task1, nil
}
