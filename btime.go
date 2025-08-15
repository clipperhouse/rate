package rate

import "time"

type btime int64

func (t btime) Time() time.Time {
	return epoch.Add(time.Duration(t))
}

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

var epoch = time.Now()

func bnow() btime {
	return btime(time.Since(epoch).Nanoseconds())
}
