// Package ntime provides a monotonic time, expressed as an
// int64 nanosecond count since an arbitrary epoch. Intended
// for applications which only require relative time measurements.
package ntime

import "time"

// Time is a wrapper around int64 to represent a monotonic time source,
// expressed as a nanosecond count since an arbitrary static epoch.
//
// Intended as an optimization to store relative time as an int64 (8 bytes),
// instead of a time.Time (24 bytes).
//
// See https://chatgpt.com/share/689f6a5d-2f64-8007-b1cc-3bdf10cfee20
type Time int64

var epoch = time.Now()

// Now returns the current relative monotonic time since
// an arbitrary static epoch. To convert to system time, use
// [ToSystemTime].
func Now() Time {
	return Time(time.Since(epoch).Nanoseconds())
}

// ToSystemTime converts the ntime.Time to a standard time.Time,
// representing the system clock time.
func (t Time) ToSystemTime() time.Time {
	return epoch.Add(time.Duration(t))
}

// various shims to look like time.Time methods

func (t Time) Add(d time.Duration) Time {
	return t + Time(d.Nanoseconds())
}

func (t Time) Sub(u Time) time.Duration {
	return time.Duration(t - u)
}

func (t Time) After(u Time) bool {
	return t > u
}

func (t Time) Before(u Time) bool {
	return t < u
}
