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
package main

import (
	"fmt"
	"time"

	"github.com/mintzhao/taskqueue"
)

func main() {
	mgr := taskqueue.NewDefaultTaskManager()
	defer mgr.Close()

	taskq := taskqueue.NewDefaultTaskQueue()
	tasker := taskqueue.NewTasker("test", taskqueue.TaskFunc(func(args ...interface{}) error {
		for idx, arg := range args {
			fmt.Printf("idx: %d, arg: %v\n", idx, arg)
		}
		return nil
	}), taskq)
	mgr.Register(tasker)

	//consumer2 := taskqueue.NewDefaultTaskConsumer("test", mgr)

	productor1 := taskqueue.NewAsyncTaskProductor(mgr, 100)
	defer productor1.Close()
	for i := 0; i <= 1; i++ {
		productor1.SendTask(taskqueue.NewTask("test", []interface{}{"productor1", i}, i))
	}

	productor2 := taskqueue.NewAsyncTaskProductor(mgr, 100)
	defer productor2.Close()
	for i := 0; i <= 10; i++ {
		productor2.SendTask(taskqueue.NewTask("test", []interface{}{"productor2", i}, i))
	}

	time.Sleep(time.Second)
	consumer1 := taskqueue.NewDefaultTaskConsumer("test", mgr, time.Second)
	defer consumer1.Close()
	for i := 0; i <= 2; i++ {
		task, err := consumer1.GetTask()
		if err != nil {
			if err == taskqueue.ErrNotFound {
				continue
			}

			panic(err)
		}

		if err := consumer1.Run(task); err != nil {
			panic(err)
		}
	}
}
