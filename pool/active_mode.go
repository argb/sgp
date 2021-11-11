package pool

import (
"fmt"
"sync"
"time"
)

/**
指导思想：公司、员工、任务
模式：积极模式，所谓积极模式就是不需要谁来分配任务，只要有活所有人都积极主动的干
老板和员工都要接客，老板其实是一个特殊员工，除了自己要干活，还可以招人
老板不管有没有活都不休息，但是干不过来可以招人，没活干就盯着会不会来活
员工不能招人，有活干就不休息，没活干就休息
*/

const (
	MaxWorker = 1000 // 最大工人数
	MaxTask = 10000 // 工厂能承接的最大任务数
	WorkLoad = 0 //工作负载，每个工人的最大工作量
	Busy int = 1
	Idle int = 0
	Expiration = 3*time.Second // 最大休假时间，超过这个时间直接开除
	AutoCheckInterval = time.Second * 5 // 测试需要调到毫秒级别才方便。
)

type Task struct {
	F         func(param ...interface{})
	arguments [] interface{}
	Id        int64
}

type Worker struct {
	p *Pool

	todo chan Task
	task Task
	capacity int
	status int
	id int64
	workOffTime time.Time
}

func (w *Worker) isBusy() bool {
	return w.status == Busy
}

type Pool struct {
	//workRoom []*Worker
	total int64 // history total
	restRoom  [] *Worker
	Tasks     chan Task
	MaxWorker int
	workingNum int

	mutex sync.Mutex
	interval time.Duration // 例行检查有没有工作不饱和的，有的话就fire掉
}

func NewPool(autoFire bool) *Pool {
	p := &Pool{
		MaxWorker: MaxWorker,
		Tasks:     make(chan Task,MaxTask),
		interval:  AutoCheckInterval,
	}

	if autoFire {
		go func() {
			t := time.NewTicker(p.interval)
			for {
				select {
				case <-t.C:
					//time.Sleep(p.interval)
					p.fireWorkers()
				}
			}
		}()
	}
	return p
}

func (p *Pool) registerWorker(w *Worker) {
	p.restRoom = append(p.restRoom, w)
}

func (p *Pool) incWorkingNum() {
	p.mutex.Lock()
	p.workingNum++
	p.mutex.Unlock()
}

func (p *Pool) decWorkingNum() {
	p.mutex.Lock()
	p.workingNum--
	p.mutex.Unlock()
}

func (p *Pool) getWorkerId() int64 {
	var total int64
	p.mutex.Lock()
	total = p.total
	total ++
	p.total = total
	p.mutex.Unlock()

	return total
}

func (p *Pool) newWorker() *Worker {
	p.incWorkingNum()

	w := &Worker{
		p: p,
		status: Idle,
		todo: make(chan Task, WorkLoad),
		capacity: WorkLoad,
		id: p.getWorkerId(),
		workOffTime: time.Now(),
	}

	//p.registerWorker(w)

	return w
}

/**
有闲着的就来干活，没有就招人，超过公司最大承受能力就不招了，慢慢干
*/
func (p *Pool) getWorker() *Worker {
	numAvailable := len(p.restRoom)
	if numAvailable > 0 {
		w := p.restRoom[numAvailable - 1]
		restRoom := p.restRoom[0 : numAvailable-1]
		p.restRoom = restRoom
		fmt.Println("worker from rest room")
		return w
	}

	// 没钱，招不了那么多人了
	if p.workingNum + numAvailable >= p.MaxWorker {
		return nil
	}
	w := p.newWorker()
	//p.registerWorker(w)
	fmt.Println("worker from new")
	return w
}

/**
老板派活了
*/
func (p *Pool) dispatchTasks() {
	go func() {
		for {

			w := p.getWorker()
			if w == nil {
				//time.Sleep(time.Second)
				fmt.Println("continue")
				// 【忙等待】，相当于自旋锁
				continue
			}

			//w.Work()
		}
	}()
}

func (p *Pool) dispatchTask(w *Worker) {
	// 工人要先就位，不然老板派了活没人干，就会阻塞在那
	w.Work()

	var counter = 0
	for task := range p.Tasks {
		if w.capacity > 0 {
			counter++
			// 分配的任务数不能超过工人的能力上限
			if counter > w.capacity {
				close(w.todo)
				return
			}
			// 领任务，放到工人自己的任务队列，a buffered channel, buffer可以避免频繁的来领任务，一次多领几个，
			// 比较适合任务小但是数量多的情况；耗时比较长的任务不合适。
			w.todo <- task
		}else {
			w.todo <- task
		}

	}
	fmt.Printf("worker %d got task\n", w.id)
	close(w.todo)
}

/**
长期闲着没事干的直接开除
平稳情况下不需要清理，因为很难有过期的worker，只有波动比较大的情况需要清理，比如一波高峰，然后任务很快恢复正常，
这时候就会产生大量worker，人手不够嘛，自然就会猛招人，但是之后任务量突然少了，大量worker就会闲置，为了节省
系统资源，直接把这些"闲人"开掉。
*/
func (p *Pool) fireWorkers() int {
	var numToFire int = 0

	p.mutex.Lock()
	if len(p.restRoom) <= 0 {
		p.mutex.Unlock()
		return numToFire
	}
	for i,w := range p.restRoom {
		if time.Now().Sub(w.workOffTime) < Expiration {
			//p.fireWorker(i)
			break
		}
		numToFire = i + 1
	}
	if numToFire == 0 {
		p.mutex.Unlock()
		return numToFire
	}
	rest := p.restRoom[numToFire : ]
	p.restRoom = rest
	fmt.Printf(" %d workers were fired, the num of rest workers is %d\n", numToFire, len(p.restRoom))
	//p.doFireWorkers(numToFire)
	p.mutex.Unlock()

	return numToFire
}

func (p *Pool) doFireWorkers(num int) {
	rest := p.restRoom[num : ]
	p.restRoom = rest
	fmt.Printf(" %d workers were fired, the num of rest workers is %d\n", num, len(p.restRoom))
}

// Work /**
/*
接客
*/
func (w *Worker) Work() {
	w.status = Busy
	//done := make(chan struct{})

	go func() {
		for {
			select {
			// 开工
			case task,ok := <-w.p.Tasks:
				if !ok {
					fmt.Println("worker: task channel closed, go home")
					// 干完活回去歇着
					w.goToRest()
					return
				}
				//fmt.Printf("worker: worker %d got tast %d. \n", w.id, task.id)
				task.F()
				//fmt.Println("worker: task finished, wait for new task.")
				// 干完活回去歇着
				//w.goToRest()
				//return
			default:
				fmt.Println("worker: without task to handle, go home")
				// 干完活回去歇着
				w.goToRest()
				return
			}
		}

	}()
}

func (p *Pool) ListenTasks(tasks chan Task) {
	p.Tasks = tasks
	go func() {
		for {
			select {
			// 开工
			case task,ok := <-p.Tasks:
				if !ok {
					fmt.Println("boos: task channel closed, go home")
					// 任务通道关闭了，不再调度
					return
				}

				task.F()
				// 老板自己干不过来，喊人来。策略：公司有闲人就喊过来，没有闲人就招人，招满为止。
				w := p.getWorker()
				// 招人招满了，人手还不够
				if w == nil {
					// 先顶着，慢慢干，慢慢招。这里可以根据具体情况设计一些策略
					// p.resize() 扩大公司规模
					continue
				}
				w.Work()
				//fmt.Printf("boos: finished the tast %d. wait for new task...\n", task.id)
				// 干完活回去歇着
				//w.goToRest()
				//return
			default:
				// 没活干，工人可以休息，老板不能休息，要盯着任务通道
				//fmt.Println("without task to handle, wait...")
			}
		}
	}()
}

func (w *Worker) goToRest() {
	// 调整下状态
	w.reset()

	// FILO, 放在队列尾部, 欺负新人，新来的多干活
	w.p.restRoom = append(w.p.restRoom, w)
}

func (w *Worker) reset() {
	w.status = Idle
	// 重置任务队列，等着老板分派新任务
	w.todo = make(chan Task, WorkLoad)
	// 下班时间记录下，方便检查，几天不上班的直接开除
	w.workOffTime = time.Now()
}

func (w *Worker) adjustCap(newCapacity int) {
	w.p.mutex.Lock()
	w.capacity = newCapacity
	w.p.mutex.Unlock()
}



