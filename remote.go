package count

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	redisTTL = time.Hour
)

type Remote interface {
	IncrBy(context.Context, string, int64) (int64, error)
}

type Redis struct {
	client redis.Cmdable
	key    string
	ttl    time.Duration
}

func MustRedis(client redis.Cmdable, key string, ttl time.Duration) *Redis {
	r, err := NewRedis(client, key, ttl)
	if err != nil {
		panic(err)
	}
	return r
}
func NewRedis(client redis.Cmdable, key string, ttl time.Duration) (*Redis, error) {
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, errors.New("redis: key empty")
	}
	if ttl == 0 {
		ttl = redisTTL
	}
	r := Redis{
		client: client,
		key:    key,
		ttl:    ttl,
	}
	return &r, nil
}
func (rt Redis) IncrBy(ctx context.Context, group string, value int64) (int64, error) {
	value, err := rt.client.IncrBy(ctx, rt.key+group, value).Result()
	if err != nil {
		return 0, err
	}
	rt.client.Expire(ctx, rt.key, rt.ttl)
	return value, nil
}
