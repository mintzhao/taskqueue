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
	"log"
)

type TaskFunc func(args ...interface{}) error

// Tasker model, every kind of task has a name and a executer
type Tasker struct {
	name     string
	executer TaskFunc
	tasks    TaskQueuer
}

// NewTasker constructed a new task model
func NewTasker(name string, executer TaskFunc, queuer TaskQueuer) *Tasker {
	if name == "" {
		return nil
	}

	return &Tasker{
		name:     name,
		executer: executer,
		tasks:    queuer,
	}
}

// Put puts task into queue
func (t *Tasker) Put(task *Task) {
	if task == nil {
		return
	}

	if task.Name != t.name {
		return
	}

	t.tasks.Push(task)
	log.Printf("tasker put task %v\n", task)
}

// Get gets task from queue
func (t *Tasker) Get() *Task {
	task := t.tasks.Pop()
	if task == nil {
		return nil
	}

	tt := task.(*Task)
	log.Printf("tasker get task: %v\n", tt)
	return tt
}

// task instance
type Task struct {
	Name     string
	Args     []interface{}
	Priority int
}

// NewTask returns a new task instance
func NewTask(name string, args []interface{}, priority int) *Task {
	return &Task{
		Name:     name,
		Args:     args,
		Priority: priority,
	}
}
