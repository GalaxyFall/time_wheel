package time_wheel

import "fmt"

type Log interface {
	Logf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type log struct{}

func (l log) Logf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (l log) Errorf(format string, args ...interface{}) {
	_ = fmt.Errorf(format, args...)
}
