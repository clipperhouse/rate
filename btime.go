package rate

import "time"

// btime is a wrapper around int64 to represent a monotonic time source.
// we use it as an optimization to store an int64 (8 bytes) on bucket,
// instead of a time.Time (24 bytes)
// https://chatgpt.com/share/689f6a5d-2f64-8007-b1cc-3bdf10cfee20
type btime int64

var epoch = time.Now()

func bnow() btime {
	return btime(time.Since(epoch).Nanoseconds())
}

func (t btime) Time() time.Time {
	return epoch.Add(time.Duration(t))
}

// various shims to look like time.Time methods

func (t btime) Add(d time.Duration) btime {
	return t + btime(d.Nanoseconds())
}

func (t btime) Sub(b btime) time.Duration {
	return time.Duration(t - b)
}

func (t btime) After(b btime) bool {
	return t > b
}

func (t btime) Before(b btime) bool {
	return t < b
}
