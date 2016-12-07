/*
Written by mint.zhao.chiu@gmail.com. github.com: https://www.github.com/mintzhao

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package taskqueue

import (
	"errors"
	"fmt"
	"sync"
	"log"
)

// TaskManager interface
type TaskManager interface {
	Register(tasker *Tasker) bool
	PutTask(task *Task) error
	GetTask(taskName string) (*Task, error)
	GetTaskFunc(taskName string) (TaskFunc, error)
	Close() error
}

// DefaultTaskManager
type DefaultTaskManager struct {
	sync.Mutex
	taskers map[string]*Tasker
}

// NewTaskManager returns a task manager
func NewDefaultTaskManager() *DefaultTaskManager {
	return &DefaultTaskManager{
		taskers: make(map[string]*Tasker),
	}
}

// Register register tasker
func (tm *DefaultTaskManager) Register(tasker *Tasker) bool {
	tm.Lock()
	defer tm.Unlock()

	if tasker == nil {
		return false
	}

	if _, ok := tm.taskers[tasker.name]; ok {
		return false
	}

	tm.taskers[tasker.name] = tasker
	return true
}

// PutTask puts task into queue
func (tm *DefaultTaskManager) PutTask(task *Task) error {
	tm.Lock()
	defer tm.Unlock()

	if task == nil {
		return errors.New("nil task")
	}

	tasker, ok := tm.taskers[task.Name]
	if !ok {
		return fmt.Errorf("no such task %s registered", task.Name)
	}

	tasker.Put(task)
	log.Printf("manager put task: %v\n", task)
	return nil
}

var (
	ErrNotFound = errors.New("no task found")
)

// GetTask gets least highest priority task
func (tm *DefaultTaskManager) GetTask(taskName string) (*Task, error) {
	tm.Lock()
	defer tm.Unlock()

	tasker, ok := tm.taskers[taskName]
	if !ok {
		return nil, fmt.Errorf("no such task %s registered", taskName)
	}
	task := tasker.Get()
	if task == nil {
		//fmt.Printf("manager get task nil\n")
		return nil, ErrNotFound
	}
	log.Printf("manager get task %s: %v\n", taskName, task)

	return task, nil
}

// GetTaskFunc gets task function
func (tm *DefaultTaskManager) GetTaskFunc(taskName string) (TaskFunc, error) {
	tm.Lock()
	defer tm.Unlock()

	tasker, ok := tm.taskers[taskName]
	if !ok {
		return nil, fmt.Errorf("no such task %s registered", taskName)
	}

	return tasker.executer, nil
}

// Close closes task manager
func (tm *DefaultTaskManager) Close() error {
	tm.taskers = make(map[string]*Tasker)
	return nil
}
