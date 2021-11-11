package main

import (
	"fmt"
	"go_routine_pool/pool"
	"time"
)

func main() {

	tasks := make(chan pool.Task)

	p := pool.NewPool(true)
	// 老板先就位，准备接客了，不然客人来了没人招呼会阻塞
	p.ListenTasks(tasks)


	// 模拟并发
	simulateTasks(p)

	// 主线程2秒后退出，每个任务只需要1秒，所以足够了
	time.Sleep(3*time.Second)

	// 又来一波高峰
	simulateTasks(p)

	close(p.Tasks)

	time.Sleep(10*time.Second)
	fmt.Println("main go routine exit!")
}

func simulateTasks(p *pool.Pool) {
	// 模拟并发
	// 每个任务需要处理1秒，在2秒的时间内处理完成300个任务
	for i:=int64(0); i<3000;i++{
		t := pool.Task{
			F: func(param ...interface{}) {
				time.Sleep(10*time.Millisecond)
				//fmt.Println("task finished. id:")
			},
			Id: i,
		}
		p.Tasks <- t
	}
}
