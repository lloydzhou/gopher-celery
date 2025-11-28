// Program metrics is a Celery worker with metrics middleware.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/go-kit/log"
	celery "github.com/marselester/gopher-celery"
	celeryredis "github.com/marselester/gopher-celery/redis"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const serverAddr = ":28080"

func main() {
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))

	m := metrics{
		total: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "tasks_total",
				Help: "How many Celery tasks processed, partitioned by task name and error.",
			},
			[]string{"task", "error"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "task_duration_seconds",
				Help: "How long it took in seconds to process a task.",
				Buckets: []float64{
					0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048, 4.096, 8.192, 16.384, 32.768, 60,
				},
			},
			[]string{"task"},
		),
	}
	prometheus.MustRegister(m.total)
	prometheus.MustRegister(m.duration)
	http.Handle("/metrics", promhttp.Handler())

	backend := celeryredis.NewBackend(nil)
	app := celery.NewApp(
		celery.WithLogger(logger),
		celery.WithMiddlewares(m.middleware),
		celery.WithBackend(backend),
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

	srv := http.Server{Addr: serverAddr}

	var g run.Group
	{
		g.Add(func() error {
			logger.Log("msg", "starting http server with metrics")
			return srv.ListenAndServe()
		}, func(err error) {
			logger.Log("msg", "shutting down http server", "err", err)
			err = srv.Shutdown(ctx)
			logger.Log("msg", "http server shut down", "err", err)
		})
	}
	{
		g.Add(func() error {
			logger.Log("msg", "waiting for tasks...")
			return app.Run(ctx)
		}, func(err error) {
			stop()
			logger.Log("msg", "celery shut down", "err", err)
		})
	}
	err := g.Run()

	logger.Log("msg", "program stopped", "err", err)
}

type metrics struct {
	total    *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func (m *metrics) middleware(next celery.TaskF) celery.TaskF {
	return func(ctx context.Context, p *celery.TaskParam) (res interface{}, err error) {
		name, ok := ctx.Value(celery.ContextKeyTaskName).(string)
		if !ok {
			return nil, fmt.Errorf("task name not found in context")
		}

		defer func(begin time.Time) {
			m.total.With(prometheus.Labels{
				"task":  name,
				"error": fmt.Sprint(err != nil),
			}).Inc()
			m.duration.With(prometheus.Labels{
				"task": name,
			}).Observe(time.Since(begin).Seconds())
		}(time.Now())

		res, err = next(ctx, p)
		return res, err
	}
}
