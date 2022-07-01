package memsto

import (
	"fmt"
	"sync"
	"time"

	"github.com/didi/nightingale/v5/src/models"
	"github.com/didi/nightingale/v5/src/server/config"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/logger"

	promstat "github.com/didi/nightingale/v5/src/server/stat"
)

type RecordRuleCacheType struct {
	statTotal       int64
	statLastUpdated int64

	sync.RWMutex
	rules map[int64]*models.RecordRule // key: rule id
}

var RecordRuleCache = RecordRuleCacheType{
	statTotal:       -1,
	statLastUpdated: -1,
	rules:           make(map[int64]*models.RecordRule),
}

func (rrc *RecordRuleCacheType) StatChanged(total, lastUpdated int64) bool {
	if rrc.statTotal == total && rrc.statLastUpdated == lastUpdated {
		return false
	}

	return true
}

func (rrc *RecordRuleCacheType) Set(m map[int64]*models.RecordRule, total, lastUpdated int64) {
	rrc.Lock()
	rrc.rules = m
	rrc.Unlock()

	// only one goroutine used, so no need lock
	rrc.statTotal = total
	rrc.statLastUpdated = lastUpdated
}

func (rrc *RecordRuleCacheType) Get(ruleId int64) *models.RecordRule {
	rrc.RLock()
	defer rrc.RUnlock()
	return rrc.rules[ruleId]
}

func (rrc *RecordRuleCacheType) GetRuleIds() []int64 {
	rrc.RLock()
	defer rrc.RUnlock()

	count := len(rrc.rules)
	list := make([]int64, 0, count)
	for ruleId := range rrc.rules {
		list = append(list, ruleId)
	}

	return list
}

func SyncRecordRules() {
	err := syncRecordRules()
	if err != nil {
		fmt.Println("failed to sync record rules:", err)
		exit(1)
	}

	go loopSyncRecordRules()
}

func loopSyncRecordRules() {
	duration := time.Duration(9000) * time.Millisecond
	for {
		time.Sleep(duration)
		if err := syncRecordRules(); err != nil {
			logger.Warning("failed to sync record rules:", err)
		}
	}
}

func syncRecordRules() error {
	start := time.Now()

	stat, err := models.RecordRuleStatistics(config.C.ClusterName)
	if err != nil {
		return errors.WithMessage(err, "failed to exec RecordRuleStatistics")
	}

	if !RecordRuleCache.StatChanged(stat.Total, stat.LastUpdated) {
		promstat.GaugeCronDuration.WithLabelValues(config.C.ClusterName, "sync_record_rules").Set(0)
		promstat.GaugeSyncNumber.WithLabelValues(config.C.ClusterName, "sync_record_rules").Set(0)
		return nil
	}

	lst, err := models.RecordRuleGetsByCluster(config.C.ClusterName)
	if err != nil {
		return errors.WithMessage(err, "failed to exec RecordRuleGetsByCluster")
	}

	m := make(map[int64]*models.RecordRule)
	for i := 0; i < len(lst); i++ {
		m[lst[i].Id] = lst[i]
	}

	RecordRuleCache.Set(m, stat.Total, stat.LastUpdated)

	ms := time.Since(start).Milliseconds()
	promstat.GaugeCronDuration.WithLabelValues(config.C.ClusterName, "sync_record_rules").Set(float64(ms))
	promstat.GaugeSyncNumber.WithLabelValues(config.C.ClusterName, "sync_record_rules").Set(float64(len(m)))

	logger.Infof("timer: sync record rules done, cost: %dms, number: %d", ms, len(m))
	return nil
}
