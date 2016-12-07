/*
Copyright Mojing Inc. 2016 All Rights Reserved.
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
	"fmt"
	"reflect"
	"testing"
	"time"
	"errors"
)

func TestDefaultTaskConsumer_GetTask(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	tasker := NewTasker("test", TaskFunc(func(args ...interface{}) error {
		for idx, arg := range args {
			fmt.Printf("idx: %d, arg: %v\n", idx, arg)
		}
		return nil
	}), taskq)
	mgr.Register(tasker)

	productor := NewAsyncTaskProductor(mgr, 100)
	defer productor.Close()

	taskSend := NewTask("test", []interface{}{"productor"}, 1)
	productor.SendTask(taskSend)

	consumer := NewDefaultTaskConsumer("test", mgr, time.Second)
	defer consumer.Close()

	taskGet, err := consumer.GetTask()
	if err != nil {
		t.Fatalf("err must be nil, but: %v", err)
	}

	if !reflect.DeepEqual(taskSend, taskGet) {
		t.Fatal("task get and send must be the same")
	}
}

func TestDefaultTaskConsumer_Run(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	testErr := errors.New("just for test")
	tasker := NewTasker("test", TaskFunc(func(args ...interface{}) error {
		return testErr
	}), taskq)
	mgr.Register(tasker)

	productor := NewAsyncTaskProductor(mgr, 100)
	defer productor.Close()

	taskSend := NewTask("test", []interface{}{"productor"}, 1)
	productor.SendTask(taskSend)

	consumer := NewDefaultTaskConsumer("test", mgr, time.Second)
	defer consumer.Close()

	taskGet, err := consumer.GetTask()
	if err != nil {
		t.Fatalf("err must be nil, but: %v", err)
	}

	if consumer.Run(taskGet) != testErr {
		t.Fatal("run result err must be the same with the testErr")
	}
}

func TestDefaultTaskConsumer_Close(t *testing.T) {
}