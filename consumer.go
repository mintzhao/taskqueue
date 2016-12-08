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
	"context"
	"errors"
	"time"
)

// TaskConsumer interface
type TaskConsumer interface {
	GetTask() (*Task, error)
	Run(task *Task) error
	Close() error
}

// DefaultTaskConsumer
type DefaultTaskConsumer struct {
	taskName string
	mgr      TaskManager
	timeout  time.Duration
}

// NewDefaultTaskConsumer return a default implement of TaskConsumer
func NewDefaultTaskConsumer(taskName string, mgr TaskManager, timeout time.Duration) *DefaultTaskConsumer {
	consumer := &DefaultTaskConsumer{
		taskName: taskName,
		mgr:      mgr,
		timeout:  timeout,
	}

	return consumer
}

var ErrTimeout = errors.New("consumer timeout")

// GetTask get least task with highest priority
func (tc *DefaultTaskConsumer) GetTask() (*Task, error) {
	ctx, _ := context.WithTimeout(context.Background(), tc.timeout)

	success := make(chan bool, 1)
	var (
		task *Task
		err  error
	)
	go func() {
		task, err = tc.mgr.GetTask(tc.taskName)

		success <- true
	}()

	select {
	case <-success:
		return task, err
	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// Run run a task with given task args
func (tc *DefaultTaskConsumer) Run(task *Task) error {
	fn, err := tc.mgr.GetTaskFunc(tc.taskName)
	if err != nil {
		return err
	}

	return fn(task.Args...)
}

// Close closes task consumer
func (tc *DefaultTaskConsumer) Close() error {
	return nil
}
