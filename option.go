package time_wheel

import "time"

type Option func(*timeWheel)

func WithLogger(log Log) Option {
	return func(wheel *timeWheel) {
		wheel.log = log
	}
}

func WithLocation(loc *time.Location) Option {
	return func(wheel *timeWheel) {
		wheel.loc = loc
	}
}

func WithTimeFunc(f func() time.Time) Option {
	return func(wheel *timeWheel) {
		wheel.timeFunc = f
	}
}
