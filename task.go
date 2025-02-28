package time_wheel

import (
	"github.com/robfig/cron/v3"
	"time"
)

type Task interface {
	Run()
}

type task struct {
	fn    Task
	key   string //任务的唯一值
	cycle int    //周期轮数 处于第几个时间轮内，可能超过了时间轮的长度
	pos   int    //当前时间轮的索引指向

	periodic bool          //是否是周期任务
	period   time.Duration //周期任务的周期

	cron             bool          //是否是cron任务
	schedule         cron.Schedule //cron任务的周期
	nextCronExecTime time.Duration //cron任务的下一次执行时间
}

// WrapperTask: 包装任务
func WrapperTask(fn func()) Task {
	j := &job{fn: fn}
	return j
}

type job struct {
	fn func()
}

func (j *job) Run() {
	j.fn()
}
