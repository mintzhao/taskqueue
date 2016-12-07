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
	"testing"
	"reflect"
)

func TestDefaultTaskManager_Register(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	tasker := NewTasker("test", TaskFunc(func(args ...interface{}) error {
		return nil
	}), taskq)

	if !mgr.Register(tasker) {
		t.Fatal("manager register function must return true at first time")
	}

	if mgr.Register(tasker) {
		t.Fatal("manager register function must return false at second time")
	}
}

func TestDefaultTaskManager_PutTask(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	tasker := NewTasker("test", TaskFunc(func(args ...interface{}) error {
		return nil
	}), taskq)

	mgr.Register(tasker)

	if mgr.PutTask(NewTask("test", []interface{}{"productor"}, 1)) != nil {
		t.Fatal("put test task must be ok")
	}

	if mgr.PutTask(NewTask("tset", []interface{}{"productor"}, 1)) == nil {
		t.Fatal("put tset task must not ok")
	}
}

func TestDefaultTaskManager_GetTask(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	tasker := NewTasker("test", TaskFunc(func(args ...interface{}) error {
		return nil
	}), taskq)
	mgr.Register(tasker)

	taskSend := NewTask("test", []interface{}{"productor"}, 1)
	mgr.PutTask(taskSend)

	taskGet, err := mgr.GetTask("test")
	if err != nil {
		t.Fatalf("must get test task: %v", err)
	}

	if !reflect.DeepEqual(taskSend, taskGet) {
		t.Fatal("two task must be the same")
	}
}

func TestDefaultTaskManager_GetTaskFunc(t *testing.T) {
	mgr := NewDefaultTaskManager()
	defer mgr.Close()

	taskq := NewDefaultTaskQueue()
	testFn := TaskFunc(func(args ...interface{}) error {
		return nil
	})
	tasker := NewTasker("test", testFn, taskq)
	mgr.Register(tasker)

	_, err := mgr.GetTaskFunc("test")
	if err != nil {
		t.Fatalf("must be nil: %v", err)
	}
}

func TestDefaultTaskManager_Close(t *testing.T) {
}