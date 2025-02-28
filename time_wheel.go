package time_wheel

import (
	"container/list"
	"github.com/robfig/cron/v3"
	"runtime/debug"
	"sync"
	"time"
)

type TimeWheeler interface {
	Start()
	//Stop:停止时间轮将会移除全部任务
	Stop()
	IsRunning() bool

	AddTask(key string, f Task, execTime time.Time)
	AddPeriodicTask(key string, f Task, Period time.Duration)
	AddCronTask(key string, spec string, f Task) error
	RemoveTask(key string)
}

//NewTimeWheel:槽大小、每次移动的时间间隔

func NewTimeWheel(slotSize int, interval time.Duration, opts ...Option) TimeWheeler {
	if slotSize <= 0 {
		slotSize = 10
	}
	if interval <= 0 {
		interval = time.Second
	}

	t := &timeWheel{
		interval:  interval,
		loc:       time.Local,
		runningMu: sync.Mutex{},
		addC:      make(chan *task),
		removeC:   make(chan string),
		stopC:     make(chan struct{}),
		slots:     make([]*list.List, slotSize),
		keys:      make(map[string]*list.Element),
		log:       log{},
	}

	for i := 0; i < slotSize; i++ { //初始化每个槽位
		t.slots[i] = list.New()
	}

	for _, opt := range opts {
		opt(t)
	}

	t.timeFunc = func() time.Time {
		return time.Now().In(t.loc)
	}

	return t
}

type timeWheel struct {
	interval  time.Duration //间隔 指针每隔多久往前移动一格
	tick      *time.Ticker  //间隔定时器
	running   bool
	runningMu sync.Mutex
	loc       *time.Location
	addC      chan *task
	removeC   chan string
	stopC     chan struct{}
	slots     []*list.List             //时间轮槽，固定长度
	currSlot  int                      //当前移动到那个槽位
	keys      map[string]*list.Element //记录任务key

	timeFunc func() time.Time
	log      Log
}

func (t *timeWheel) Start() {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	if t.isRunning() {
		return
	}

	t.running = true
	t.tick = time.NewTicker(t.interval)

	t.log.Logf("time wheel started \n")

	t.safeGo(func() {
		t.run()
	})
}

func (t *timeWheel) Stop() {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	if !t.isRunning() {
		return
	}

	t.tick.Stop()
	t.running = false
	t.stopC <- struct{}{}
}

func (t *timeWheel) IsRunning() bool {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	return t.isRunning()
}

func (t *timeWheel) AddTask(key string, f Task, execTime time.Time) {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	//计算周期轮数和索引
	cycle, pos := t.caclCycleAndPos(execTime)

	task := &task{
		fn:    f,
		key:   key,
		cycle: cycle,
		pos:   pos,
	}

	if !t.isRunning() {
		t.add(task)
		return
	}

	t.addC <- task
}

func (t *timeWheel) AddPeriodicTask(key string, f Task, period time.Duration) {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	//计算下一次执行的周期轮数和索引
	execTime := t.timeFunc().Add(period)
	cycle, pos := t.caclCycleAndPos(execTime)

	task := &task{
		fn:       f,
		key:      key,
		cycle:    cycle,
		pos:      pos,
		periodic: true,
		period:   period,
	}

	if !t.isRunning() {
		t.add(task)
		return
	}

	t.addC <- task
}

func (t *timeWheel) AddCronTask(key string, spec string, f Task) error {
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(spec)
	if err != nil {
		return err
	}

	nextExecTime := schedule.Next(t.timeFunc())
	cycle, pos := t.caclCycleAndPos(nextExecTime)

	t.log.Logf("add cron task: %s, time %d, next exec time: %d \n", key, t.timeFunc().Unix(), nextExecTime.Unix())

	task := &task{
		fn:               f,
		key:              key,
		cycle:            cycle,
		pos:              pos,
		cron:             true,
		schedule:         schedule,
		nextCronExecTime: time.Duration(nextExecTime.Unix()),
	}

	if !t.isRunning() {
		t.add(task)
		return nil
	}

	t.addC <- task
	return nil
}

func (t *timeWheel) RemoveTask(key string) {
	t.runningMu.Lock()
	defer t.runningMu.Unlock()

	if !t.isRunning() {
		t.remove(key)
		return
	}

	t.removeC <- key
}

func (t *timeWheel) isRunning() bool {
	return t.running
}

func (t *timeWheel) run() {
	for {
		select {
		case <-t.tick.C:
			t.log.Logf("time wheel tick currSlot %d \n", t.currSlot)
			t.execute()
			t.incr()
		case task := <-t.addC:
			t.add(task)
		case key := <-t.removeC:
			t.remove(key)
		case <-t.stopC:
			t.log.Logf("time wheel stopped \n")
			return
		}
	}
}

// incr : 时间轮指针向前移动一格
func (t *timeWheel) incr() {
	t.currSlot = (t.currSlot + 1) % len(t.slots)
}

func (t *timeWheel) add(_task *task) {
	t.log.Logf("add task %+v \n", _task)

	//如果已经存在该任务，则删除
	if old, ok := t.keys[_task.key]; ok {
		t.log.Logf("remove old task %s \n", old.Value.(*task).key)
		t.slots[old.Value.(*task).pos].Remove(old)
	}

	//将任务添加到时间轮槽中
	elem := t.slots[_task.pos].PushBack(_task)
	//将任务的key和元素关联起来
	t.keys[_task.key] = elem
}

func (t *timeWheel) remove(key string) {
	elem, ok := t.keys[key]
	if !ok {
		return
	}

	delete(t.keys, key)
	task := elem.Value.(*task)
	//根据任务的pos删除任务
	t.slots[task.pos].Remove(elem)
}

// 计算任务的周期数和索引
func (t *timeWheel) caclCycleAndPos(execTime time.Time) (int, int) {
	//如果已经过期则立即执行
	if execTime.Before(t.timeFunc()) {
		t.log.Logf("task %s is expired,need exec right now \n", execTime)
		return 0, t.currSlot
	}

	delay := execTime.Sub(t.timeFunc())

	//多少个轮数 延时时间除以时间轮的间隔
	cycle := int(delay) / (int(t.interval) * len(t.slots))
	pos := (t.currSlot + int(delay)/int(t.interval)) % len(t.slots)

	return cycle, pos
}
func (t *timeWheel) reschedulePeriodTask(_task *task) {
	delay := _task.period
	_task.cycle = int(delay) / (int(t.interval) * len(t.slots))
	_task.pos = (t.currSlot + int(delay)/int(t.interval)) % len(t.slots)

	t.log.Logf("reschedulePeriodTask task %s cycle %d pos %d \n", _task.key, _task.cycle, _task.pos)

	t.add(_task)
}

func (t *timeWheel) rescheduleCronTask(_task *task) {
	nextExecTime := _task.schedule.Next(t.timeFunc())
	_task.nextCronExecTime = time.Duration(nextExecTime.Unix())

	delay := nextExecTime.Sub(t.timeFunc())

	//这里需要特别处理一下加+1 cron的调度next与 先调用执行槽任务再移动有关
	_task.cycle = int(delay) / (int(t.interval) * len(t.slots))
	_task.pos = (t.currSlot + 1 + int(delay)/int(t.interval)) % len(t.slots)

	t.log.Logf("rescheduleCronTask task %s time %d nextExecTime %d cycle %d pos %d \n", _task.key,
		t.timeFunc().Unix(), _task.nextCronExecTime, _task.cycle, _task.pos)

	t.add(_task)
}

func (t *timeWheel) execute() {
	list := t.slots[t.currSlot]

	for v := list.Front(); v != nil; {
		task := v.Value.(*task)
		//如果任务的周期数大于0，则表示任务还没有执行完
		if task.cycle > 0 {
			//每次的槽位指针经过已经是一个时间轮
			task.cycle--
			v = v.Next()
			continue
		}

		//执行任务
		t.safeGo(func() {
			t.log.Logf("task %s time %s execute \n", task.key, t.timeFunc())
			task.fn.Run()
		})

		t.remove(task.key)
		//如果是周期任务，则重新计算下一次执行的时间入队
		if task.periodic {
			t.reschedulePeriodTask(task)
		}
		if task.cron {
			t.rescheduleCronTask(task)
		}
		v = v.Next()
	}
}

func (t *timeWheel) safeGo(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.log.Errorf("time wheel execute task panic,stack[%v] \n", debug.Stack())
			}
		}()

		fn()
	}()
}
