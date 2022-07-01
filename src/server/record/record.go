package record

import (
	"context"
	"time"

	"github.com/didi/nightingale/v5/src/server/memsto"
)

func Start(ctx context.Context) error {
	duration := time.Duration(9000) * time.Millisecond

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				Workers.Build(memsto.RecordRuleCache.GetRuleIds())
			}
		}
	}()
	return nil
}
