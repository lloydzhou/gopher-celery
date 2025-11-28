// Program consumer receives "myproject.mytask" tasks from "important" queue.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/go-kit/log"
	celery "github.com/marselester/gopher-celery"
	celeryrabbitmq "github.com/marselester/gopher-celery/rabbitmq"

	// redisbackend "github.com/marselester/gopher-celery/redis"
	redisbackend "github.com/marselester/gopher-celery/goredis"
)

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	broker, e := celeryrabbitmq.NewBroker(celeryrabbitmq.WithAmqpUri("amqp://guest:guest@localhost:5672/"))
	if e != nil {
		logger.Log("msg", "failed to create broker", "err", e)
		return
	}
	app := celery.NewApp(
		celery.WithBroker(broker),
		celery.WithLogger(logger),
		celery.WithBackend(redisbackend.NewBackend(nil)),
	)
	app.Register(
		"myproject.mytask",
		"important",
		func(ctx context.Context, p *celery.TaskParam) (interface{}, error) {
			p.NameArgs("a", "b")
			fmt.Printf("received a=%s b=%s\n", p.MustString("a"), p.MustString("b"))
			return p.MustString("a") + p.MustString("b"), nil
		},
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logger.Log("msg", "waiting for tasks...")
	err := app.Run(ctx)
	logger.Log("msg", "program stopped", "err", err)
}
