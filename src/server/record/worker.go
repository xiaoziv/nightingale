package record

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/didi/nightingale/v5/src/models"
	"github.com/didi/nightingale/v5/src/server/common"
	"github.com/didi/nightingale/v5/src/server/common/conv"
	"github.com/didi/nightingale/v5/src/server/memsto"
	"github.com/didi/nightingale/v5/src/server/reader"
	"github.com/didi/nightingale/v5/src/server/writer"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/toolkits/pkg/logger"
	"github.com/toolkits/pkg/str"
)

type RuleEval struct {
	rule    *models.RecordRule
	ruleTag map[string]string // ruleTag cache, no necessary to split tags every evaluation every time series
	quit    chan struct{}
}

func (r RuleEval) Stop() {
	logger.Infof("record rule eval:%d stopping", r.RuleID())
	close(r.quit)
}

func (r RuleEval) RuleID() int64 {
	return r.rule.Id
}

func (r RuleEval) Start() {
	logger.Infof("record rule eval:%d starting", r.RuleID())

	for {
		select {
		case <-r.quit:
			return
		default:
			r.Work()
			logger.Debugf("record rule executed，rule_id=%d", r.RuleID())
			interval := r.rule.PromEvalInterval
			if interval <= 10 {
				interval = 10
			}
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

func (r RuleEval) Work() {
	promql := strings.TrimSpace(r.rule.PromQl)
	if promql == "" {
		logger.Errorf("record rule_eval:%d promql is blank", r.RuleID())
		return
	}

	var (
		value    model.Value
		warnings reader.Warnings
		err      error
	)

	// todo, rule evaluate stats cache to redis?
	value, warnings, err = reader.Reader.Client.Query(context.Background(), promql, time.Now())
	if err != nil {
		logger.Errorf("record rule_eval:%d promql:%s, error:%v", r.RuleID(), promql, err)
		common.NotifyToMaintainer(err, "error occurs when querying prometheus")
		return
	}

	if len(warnings) > 0 {
		logger.Errorf("record rule_eval:%d promql:%s, warnings:%v", r.RuleID(), promql, warnings)
		return
	}

	lst := conv.ConvertVectors(value)

	for _, vector := range lst {
		// build metrics
		pt := &prompb.TimeSeries{}
		pt.Samples = append(pt.Samples, prompb.Sample{
			// use ms
			Timestamp: vector.Timestamp * 1000,
			Value:     vector.Value,
		})

		// handle metric name
		pt.Labels = append(pt.Labels, &prompb.Label{
			Name:  model.MetricNameLabel,
			Value: r.rule.Metric,
		})

		// handle series tags
		for label, value := range vector.Labels {
			if label == model.MetricNameLabel {
				continue
			}
			// skip rule tags
			if _, has := r.ruleTag[string(label)]; has {
				continue
			}
			pt.Labels = append(pt.Labels, &prompb.Label{
				Name:  string(label),
				Value: string(value),
			})
		}

		// handle rule tags
		for k, v := range r.ruleTag {
			pt.Labels = append(pt.Labels, &prompb.Label{
				Name:  k,
				Value: v,
			})
		}

		writer.Writers.PushSample(r.rule.Metric, pt)
	}
}

type WorkersType struct {
	rules map[string]RuleEval
}

var Workers = &WorkersType{rules: make(map[string]RuleEval)}

func (ws *WorkersType) Build(rids []int64) {
	rules := make(map[string]*models.RecordRule)

	for i := 0; i < len(rids); i++ {
		rule := memsto.RecordRuleCache.Get(rids[i])
		if rule == nil {
			continue
		}

		hash := str.MD5(fmt.Sprintf("%d_%s_%s_%d",
			rule.Id,
			rule.Metric,
			rule.PromQl,
			rule.PromEvalInterval,
		))

		rules[hash] = rule
	}

	// stop old
	for hash := range Workers.rules {
		if _, has := rules[hash]; !has {
			Workers.rules[hash].Stop()
			delete(Workers.rules, hash)
		}
	}

	// start new
	for hash, rule := range rules {
		if _, has := Workers.rules[hash]; has {
			continue
		}

		ruleTag := make(map[string]string)
		// handle rule tags
		for _, tag := range rule.AppendTagsJSON {
			arr := strings.SplitN(tag, "=", 2)
			ruleTag[arr[0]] = arr[1]
		}

		re := RuleEval{
			rule:    rule,
			ruleTag: ruleTag,
			quit:    make(chan struct{}),
		}

		go re.Start()
		Workers.rules[hash] = re
	}
}
