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
	"container/heap"
	"sync"
	"log"
)

type TaskQueuer interface {
	Push(x interface{})
	Pop() interface{}
}

type DefaultTaskQueue struct {
	sync.RWMutex

	tasks []*Task
}

func (tq *DefaultTaskQueue) Len() int {
	tq.RLock()
	defer tq.RUnlock()

	return len(tq.tasks)
}

func (tq *DefaultTaskQueue) Less(i, j int) bool {
	tq.RLock()
	defer tq.RUnlock()

	return tq.tasks[i].Priority > tq.tasks[j].Priority
}

func (tq *DefaultTaskQueue) Swap(i, j int) {
	tq.Lock()
	tq.Unlock()

	tq.tasks[i], tq.tasks[j] = tq.tasks[j], tq.tasks[i]
}

func (tq *DefaultTaskQueue) Push(x interface{}) {
	tq.Lock()
	defer tq.Unlock()

	task := x.(*Task)
	tq.tasks = append(tq.tasks, task)

	log.Printf("queue push task: %v, tqLen: %v\n", x, len(tq.tasks))
}

func (tq *DefaultTaskQueue) Pop() interface{} {
	tq.Lock()
	defer tq.Unlock()

	tasksLen := len(tq.tasks)
	if tasksLen == 0 {
		log.Println("task queue length is 0, can't pop")
		return nil
	}

	task := tq.tasks[tasksLen-1]
	tq.tasks = tq.tasks[:tasksLen-1]

	log.Printf("task queue pop task: %v\n", task)
	return task
}

// NewDefaultTaskQueue returns a task queue implemented by heap
func NewDefaultTaskQueue() *DefaultTaskQueue {
	defTQ := &DefaultTaskQueue{
		tasks: make([]*Task, 0),
	}

	heap.Init(defTQ)
	return defTQ
}
