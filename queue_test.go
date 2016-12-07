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

func TestDefaultTaskQueue_Push(t *testing.T) {
	queue := NewDefaultTaskQueue()

	taskSend := NewTask("test", []interface{}{"productor"}, 1)
	queue.Push(taskSend)

	if queue.Len() != 1 {
		t.Fatal("queue length must be 1")
	}
}

func TestDefaultTaskQueue_Pop(t *testing.T) {
	queue := NewDefaultTaskQueue()

	taskSend := NewTask("test", []interface{}{"productor"}, 1)
	queue.Push(taskSend)

	taskGet := queue.Pop().(*Task)

	if !reflect.DeepEqual(taskSend, taskGet) {
		t.Fatal("two task must be the same")
	}
}