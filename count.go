package count

import (
	"context"
	"sync/atomic"
)

type Counter struct {
	remote Remote

	// timer    time.Timer
	// latestAt time.Time
	// ewma     ewma.MovingAverage

	synced int64
	unsync int64
}

func MustCounter(remote Remote) *Counter {
	ct, err := NewCounter(remote)
	if err != nil {
		panic(err)
	}
	return ct
}
func NewCounter(remote Remote) (*Counter, error) {
	counter := Counter{remote: remote}
	return &counter, nil
}

func (ct *Counter) Count(ctx context.Context) int64 {
	if ct == nil {
		return 0
	}
	return atomic.LoadInt64(&ct.unsync)
}

func (ct *Counter) Reset(ctx context.Context) (synced, unsync int64) {
	if ct == nil {
		return 0, 0
	}
	unsync = atomic.SwapInt64(&ct.unsync, 0)
	synced = atomic.SwapInt64(&ct.synced, 0)
	if ct.remote != nil {
		ct.remote.Reset(ctx)
	}
	return
}
func (ct *Counter) Sync(ctx context.Context) (remote int64, err error) {
	if ct == nil || ct.remote == nil {
		return 0, nil
	}
	var (
		unsync = atomic.LoadInt64(&ct.unsync)
		synced = ct.synced
	)
	remote, err = ct.remote.IncrBy(ctx, unsync-synced)
	if err != nil {
		return 0, err
	}
	atomic.AddInt64(&ct.unsync, remote-unsync)
	ct.synced = remote
	return remote, err
}

func (ct *Counter) Hit(ctx context.Context) {
	if ct == nil {
		return
	}
	atomic.AddInt64(&ct.unsync, 1)
	// now := time.Now()
	// ct.ewma.Add(float64(now.Sub(ct.latestAt)))
	// ct.latestAt = now
}
func (ct *Counter) Limit(ctx context.Context, limit int64) (bool, error) {
	if ct == nil {
		return true, nil
	}
	unsync := atomic.LoadInt64(&ct.unsync)
	if unsync >= limit {
		return false, nil
	}

	added := atomic.AddInt64(&ct.unsync, 1)
	switch {
	case added < limit:
		// now := time.Now()
		// ct.ewma.Add(float64(now.Sub(ct.latestAt)))
		// ct.latestAt = now
		return true, nil
	case added > limit:
		atomic.AddInt64(&ct.unsync, -1)
		return false, nil
	case added == limit:
		if _, err := ct.Sync(ctx); err != nil {
			return true, err
		}
		return true, nil
	default:
		panic("impossibility")
	}
}
func (ct *Counter) miss(ctx context.Context) {
	if ct == nil {
		return
	}
	atomic.AddInt64(&ct.unsync, -1)
	// now := time.Now()
	// ct.ewma.Add(float64(now.Sub(ct.latestAt)))
	// ct.latestAt = now
}
