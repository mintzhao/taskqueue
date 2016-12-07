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

// TaskProductor interface
type TaskProductor interface {
	SendTask(task *Task) error
	Close() error
}

// AsyncTaskProductor
type AsyncTaskProductor struct {
	mgr      TaskManager
	messages chan *Task
	close    chan int
}

// NewAsyncTaskProductor returns a new async task productor
func NewAsyncTaskProductor(mgr TaskManager, cacheLen int) *AsyncTaskProductor {
	productor := &AsyncTaskProductor{
		mgr:      mgr,
		messages: make(chan *Task, cacheLen),
		close:    make(chan int),
	}
	go productor.asyncSendTasks()

	return productor
}

// SendTask send task to inner channel
func (tp *AsyncTaskProductor) SendTask(task *Task) error {
	tp.messages <- task
	return nil
}

// asyncSendTasks
func (tp *AsyncTaskProductor) asyncSendTasks() {
	for {
		select {
		case task := <-tp.messages:
			tp.mgr.PutTask(task)
		case <-tp.close:
			close(tp.messages)
			close(tp.close)
			return
		}
	}
}

// Close closes productor
func (tp *AsyncTaskProductor) Close() error {
	tp.close <- 1

	return nil
}
