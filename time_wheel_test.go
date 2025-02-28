package time_wheel

import (
	"container/list"
	"sync"
	"testing"
	"time"
)

const testFuncTime = 1740576360

func testTimeWheel(slotSize int, interval time.Duration, opts ...Option) *timeWheel {
	if slotSize <= 0 {
		slotSize = 10
	}
	if interval <= 0 {
		interval = time.Second
	}

	t := &timeWheel{
		interval:  interval,
		tick:      time.NewTicker(interval),
		loc:       time.Local,
		runningMu: sync.Mutex{},
		addC:      make(chan *task),
		removeC:   make(chan string),
		slots:     make([]*list.List, slotSize),
		keys:      make(map[string]*list.Element),
		log:       log{},
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

type testLog struct {
	tx *testing.T
}

func (l *testLog) Logf(format string, args ...interface{}) {
	l.tx.Logf(format, args...)
}

func (l *testLog) Errorf(format string, args ...interface{}) {
	l.tx.Errorf(format, args...)
}

func TestCaclCycleAndPos(t *testing.T) {

	opts := []Option{
		WithTimeFunc(func() time.Time {
			return time.Unix(testFuncTime, 0)
		}),
	}

	twheel := testTimeWheel(10, time.Second, opts...)

	t.Log("start caclCycleAndPos test... \n")

	//生成表格驱动测试
	tests := []struct {
		execTime time.Time
		cycle    int
		pos      int
	}{
		{time.Unix(testFuncTime+5, 0), 0, 5},
		{time.Unix(testFuncTime+15, 0), 1, 5},
		{time.Unix(testFuncTime+10, 0), 1, 0},
		{time.Unix(testFuncTime, 0), 0, 0},
		{time.Unix(testFuncTime-20, 0), 0, 0},
	}

	for _, test := range tests {
		cycle, pos := twheel.caclCycleAndPos(test.execTime)
		if cycle != test.cycle || pos != test.pos {
			t.Fatalf("caclCycleAndPos error cycle:%d test:%d pos:%d test%d", cycle, test.cycle, pos, test.pos)
		}
	}

}

func TestTimeWheel(t *testing.T) {
	shanghaiLocation, _ := time.LoadLocation("Asia/Shanghai")
	tWheel := NewTimeWheel(5, time.Second, WithLocation(shanghaiLocation))

	t.Log("start time wheel test... \n")

	testfunc := func() {
		t.Log("test func run \n")
	}

	tWheel.AddTask("test1", WrapperTask(testfunc), time.Now().In(shanghaiLocation).Add(time.Second))
	tWheel.AddTask("test5", WrapperTask(testfunc), time.Now().In(shanghaiLocation).Add(time.Second*5))

	tWheel.Start()
	<-time.After(6 * time.Second)
}

func TestTimeWheelPeriodic(t *testing.T) {
	shanghaiLocation, _ := time.LoadLocation("Asia/Shanghai")
	tWheel := NewTimeWheel(3, time.Second, WithLocation(shanghaiLocation))

	t.Log("start time wheel periodic test... \n")

	testfunc := func() {}

	tWheel.Start()

	tWheel.AddPeriodicTask("test Periodic", WrapperTask(testfunc), time.Second*5)

	<-time.After(20 * time.Second)
}

func TestTimeWheelCron(t *testing.T) {
	shanghaiLocation, _ := time.LoadLocation("Asia/Shanghai")
	tWheel := NewTimeWheel(3, time.Second, WithLocation(shanghaiLocation))

	t.Log("start time wheel cron test... \n")

	testfunc := func() {}

	tWheel.Start()

	// 每5秒执行一次
	if err := tWheel.AddCronTask("test corn", "*/3 * * * * *", WrapperTask(testfunc)); err != nil {
		t.Fatalf(err.Error())
	}

	<-time.After(20 * time.Second)
}

func TestTimeWheelCron2(t *testing.T) {
	shanghaiLocation, _ := time.LoadLocation("Asia/Shanghai")
	tWheel := NewTimeWheel(3, time.Second, WithLocation(shanghaiLocation))

	t.Log("start time wheel cron 2 test... \n")

	testfunc := func() {}

	tWheel.Start()

	// 每分钟的5 25 50秒执行一次
	if err := tWheel.AddCronTask("test corn", "5,25,50 * * * * ?", WrapperTask(testfunc)); err != nil {
		t.Fatalf(err.Error())
	}

	<-time.After(60 * time.Second)
}

func TestTaskPanic(t *testing.T) {

	t.Log("start task panic test... \n")

	tWheel := NewTimeWheel(10, time.Second)

	testfunc := func() {
		panic("test panic")
	}

	tWheel.Start()
	tWheel.AddTask("test1", WrapperTask(testfunc), time.Now().Add(time.Second*2))

	<-time.After(4 * time.Second)
}

func TestTimeWheelState(t *testing.T) {
	tWheel := NewTimeWheel(10, time.Second, WithLogger(&testLog{tx: t}))

	t.Log("start time wheel state test... \n")

	if tWheel.IsRunning() {
		t.Fatalf("time wheel is running \n")
	}

	time.Sleep(time.Second * 3)

	tWheel.Start()
	if !tWheel.IsRunning() {
		t.Fatalf("time wheel not is running \n")
	}

	time.Sleep(time.Second * 3)

	//停止测试
	tWheel.Stop()
	if tWheel.IsRunning() {
		t.Fatalf("time wheel is running \n")
	}

	time.Sleep(time.Second * 3)

	tWheel.Start()
	if !tWheel.IsRunning() {
		t.Fatalf("time wheel not is running \n")
	}

	time.Sleep(time.Second * 3)
}
