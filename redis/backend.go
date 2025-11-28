package redis

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// Backend implements celery.Backend and manages its own Redis connection pool.
type Backend struct {
	pool *redis.Pool
}

// NewBackend creates a Backend with its own Redis pool.
// If pool is nil, connects to redis://localhost by default.
func NewBackend(pool *redis.Pool) *Backend {
	if pool == nil {
		pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.DialURL("redis://localhost")
			},
		}
	}
	return &Backend{pool: pool}
}

func (b *Backend) Store(key string, value []byte) error {
	conn := b.pool.Get()
	defer conn.Close()

	if err := conn.Send("SETEX", fmt.Sprintf("celery-task-meta-%s", key), 86400, value); err != nil {
		return err
	}

	if err := conn.Send("PUBLISH", fmt.Sprintf("celery-task-meta-%s", key), value); err != nil {
		return err
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	_, err := conn.Receive()

	return err
}

func (b *Backend) Load(key string) ([]byte, error) {
	conn := b.pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do(
		"GET",
		redis.Args{}.AddFlat(fmt.Sprintf("celery-task-meta-%s", key)),
	))
}
