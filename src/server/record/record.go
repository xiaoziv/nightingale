package record

import (
	"context"
	"time"

	"github.com/didi/nightingale/v5/src/server/memsto"
	"github.com/didi/nightingale/v5/src/server/naming"
	"github.com/toolkits/pkg/logger"
)

func Start(ctx context.Context) error {
	duration := time.Duration(9000) * time.Millisecond

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(duration):
				// only leader can evaluate record rules
				isLeader, err := naming.IamLeader()
				if err != nil {
					logger.Errorf("record rule, failed to get leader: %v", err)
					continue
				}
				if isLeader {
					Workers.Build(memsto.RecordRuleCache.GetRuleIds())
				} else {
					logger.Debugf("record rule, not leader, skip evaluate")
				}
			}
		}
	}()
	return nil
}
