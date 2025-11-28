package celery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/marselester/gopher-celery/protocol"
)

func BackendMiddleware(backend Backend) func(next TaskF) TaskF {
	return func(f TaskF) TaskF {
		return func(ctx context.Context, p *TaskParam) (interface{}, error) {
			if err := setResult(ctx, backend, protocol.STARTED, nil); err != nil {
				return nil, err
			}

			res, err := f(ctx, p)

			if err == nil {
				var finalRes interface{}
				if res != nil {
					finalRes = res
				}
				if err1 := setResult(ctx, backend, protocol.SUCCESS, finalRes); err1 != nil {
					return finalRes, err1
				}
				return finalRes, nil
			}

			if err1 := setResult(ctx, backend, protocol.FAILURE, packError(err.Error())); err1 != nil {
				err = fmt.Errorf("%w, %s", err, err1)
			}

			return nil, err
		}
	}
}

func packError(err string) interface{} {
	return map[string]interface{}{
		"exc_type":    "GopherError",
		"exc_message": err,
	}
}

func setResult(ctx context.Context, backend Backend, status protocol.Status, result interface{}) error {
	taskID, ok := ctx.Value(ContextKeyTaskID).(string)
	if !ok {
		return nil
	}

	msg := &protocol.Result{
		ID:        taskID,
		Status:    string(status),
		Traceback: nil,
		Result:    result,
		Children:  []interface{}{},
		DateDone:  time.Now().UTC().Format(time.RFC3339Nano),
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return backend.Store(taskID, payload)
}

var ErrTypeAssertion = fmt.Errorf("type assertion failed")
