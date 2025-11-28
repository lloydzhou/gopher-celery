package goredis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Backend implements celery.Backend and manages its own Redis client and context.
type Backend struct {
	client *redis.Client
	ctx    context.Context
}

// NewBackend creates a Backend with its own Redis client and context.
// If client is nil, connects to redis://localhost by default.
func NewBackend(client *redis.Client) *Backend {
	if client == nil {
		client = redis.NewClient(&redis.Options{})
	}
	return &Backend{client: client, ctx: context.Background()}
}

func (b *Backend) Store(key string, value []byte) error {
	pipe := b.client.Pipeline()

	pipe.Set(b.ctx, fmt.Sprintf("celery-task-meta-%s", key), value, 24*time.Hour)
	pipe.Publish(b.ctx, fmt.Sprintf("celery-task-meta-%s", key), value)

	_, err := pipe.Exec(b.ctx)

	return err
}

func (b *Backend) Load(key string) ([]byte, error) {
	return b.client.Get(b.ctx, fmt.Sprintf("celery-task-meta-%s", key)).Bytes()
}
