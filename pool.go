package count

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Pool struct {
	client redis.Cmdable
	prefix string

	counters map[string]*Counter
}

type PoolOption func(*Pool)

func WithPrefix(s string) PoolOption {
	return func(pl *Pool) {
		pl.prefix = s
	}
}

func MustPool(client redis.Cmdable, opts ...PoolOption) *Pool {
	pl, err := NewPool(client, opts...)
	if err != nil {
		panic(err)
	}
	return pl
}
func NewPool(client redis.Cmdable, opts ...PoolOption) (*Pool, error) {
	pl := Pool{
		client:   client,
		counters: make(map[string]*Counter),
	}
	for ix := range opts {
		opts[ix](&pl)
	}
	return &pl, nil
}

func (pl *Pool) Get(key string) *Counter {
	return pl.counters[key]
}
func (pl *Pool) Touch(key string, ttl time.Duration) *Counter {
	if _, ok := pl.counters[key]; !ok {
		rt, err := NewRedis(pl.client, pl.prefix+key, ttl)
		if err != nil {
			return nil
		}
		ct, err := NewCounter(rt)
		if err != nil {
			return nil
		}
		pl.counters[key] = ct
	}
	return pl.counters[key]
}

func (pl *Pool) ForEach(f func(key string, counter *Counter) bool) {
	for key := range pl.counters {
		if !f(key, pl.counters[key]) {
			return
		}
	}
}

func (pl *Pool) Hits(ctx context.Context, keys ...string) {
	for ix := range keys {
		if _, ok := pl.counters[keys[ix]]; ok {
			pl.counters[keys[ix]].Hit(ctx)
		}
	}
}
func (pl *Pool) Limits(ctx context.Context, limits map[string]int64) (bool, error) {
	hitted := make([]string, len(limits))
	for key := range limits {
		if _, ok := pl.counters[key]; !ok {
			continue
		}
		ok, err := pl.counters[key].Limit(ctx, limits[key])
		if err != nil || !ok {
			for ix := range hitted {
				pl.counters[hitted[ix]].miss(ctx)
			}
			return false, err
		}
		hitted = append(hitted, key)
	}
	return true, nil
}
