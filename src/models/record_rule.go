package models

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
	"strings"
	"time"
)

type RecordRule struct {
	Id               int64    `json:"id" gorm:"primaryKey"`
	GroupId          int64    `json:"group_id"`           // busi group id
	Cluster          string   `json:"cluster"`            // take effect by cluster
	Note             string   `json:"note"`               // just comment of current record rule
	Metric           string   `json:"metric"`             // metric name of current record rule
	PromQl           string   `json:"promql"`             // promql of current record rule
	PromEvalInterval int      `json:"prom_eval_interval"` // promql eval interval, unit:s
	AppendTags       string   `json:"-"`
	AppendTagsJSON   []string `json:"append_tags" gorm:"-"` // for fe
	Disabled         int      `json:"disabled"`
	CreateAt         int64    `json:"create_at"`
	CreateBy         string   `json:"create_by"`
	UpdateAt         int64    `json:"update_at"`
	UpdateBy         string   `json:"update_by"`
}

func (rr *RecordRule) TableName() string {
	return "record_rule"
}

func (rr *RecordRule) Verify() error {
	if rr.GroupId <= 0 {
		return fmt.Errorf("GroupId(%d) invalid", rr.GroupId)
	}

	if rr.Cluster == "" {
		return errors.New("Cluster is blank")
	}

	if str.Dangerous(rr.Metric) {
		return errors.New("Metric has invalid characters")
	}

	if rr.Metric == "" {
		return errors.New("Metric is blank")
	}

	if rr.PromQl == "" {
		return errors.New("PromQl is blank")
	}

	if rr.PromEvalInterval <= 15 {
		rr.PromEvalInterval = 15
	}

	rr.AppendTags = strings.TrimSpace(rr.AppendTags)
	arr := strings.Fields(rr.AppendTags)
	for i := 0; i < len(arr); i++ {
		if len(strings.Split(arr[i], "=")) != 2 {
			return fmt.Errorf("AppendTags(%s) invalid", arr[i])
		}
	}
	return nil
}

func (rr *RecordRule) Add() error {
	if err := rr.Verify(); err != nil {
		return err
	}

	exists, err := RecordRuleExists("cluster=? and metric=?", rr.Cluster, rr.Metric)
	if err != nil {
		return err
	}

	if exists {
		return errors.New("RecordRule already exists, maybe in other busi-group, please check or contact admin")
	}

	now := time.Now().Unix()
	rr.CreateAt = now
	rr.UpdateAt = now

	return Insert(rr)
}

func (rr *RecordRule) Update(rrf RecordRule) error {
	if rr.Metric != rrf.Metric {
		exists, err := RecordRuleExists("cluster=? and metric=? and id <> ?", rr.Cluster, rrf.Metric, rr.Id)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("RecordRule already exists")
		}
	}

	rrf.FE2DB()
	rrf.Id = rr.Id
	rrf.GroupId = rr.GroupId
	rrf.CreateAt = rr.CreateAt
	rrf.CreateBy = rr.CreateBy
	rrf.UpdateAt = rr.UpdateAt

	err := rrf.Verify()
	if err != nil {
		return err
	}
	return DB().Model(rr).Select("*").Updates(rrf).Error
}

func (rr *RecordRule) UpdateFieldsMap(fields map[string]interface{}) error {
	return DB().Model(rr).Updates(fields).Error
}

func (rr *RecordRule) FE2DB() {
	rr.AppendTags = strings.Join(rr.AppendTagsJSON, " ")
}

func (rr *RecordRule) DB2FE() {
	rr.AppendTagsJSON = strings.Fields(rr.AppendTags)
}

func RecordRuleDels(ids []int64) error {
	return DB().Where("id in (?)", ids).Delete(&RecordRule{}).Error
}

func RecordRuleExists(where string, args ...interface{}) (bool, error) {
	return Exists(DB().Model(&RecordRule{}).Where(where, args...))
}

func RecordRuleGets(groupId int64) ([]RecordRule, error) {
	session := DB().Where("group_id = ?", groupId).Order("metric")

	var lst []RecordRule
	err := session.Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}
	return lst, err
}

func RecordRuleGetsByCluster(cluster string) ([]*RecordRule, error) {
	session := DB().Where("disabled = ?", 0)

	if cluster != "" {
		session = session.Where("cluster = ?", cluster)
	}

	var lst []*RecordRule
	err := session.Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}
	return lst, err
}

func RecordRulesGetsBy(query string) ([]RecordRule, error) {
	session := DB().Where("disabled = ?", 0)

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			for i := 0; i < len(arr); i++ {
				qarg := "%" + arr[i] + "%"
				session = session.Where("append_tags like ?", qarg)
			}
		}
	}

	var lst []RecordRule
	err := session.Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}
	return lst, err
}

func RecordRuleGet(where string, args ...interface{}) (*RecordRule, error) {
	var lst []*RecordRule
	err := DB().Where(where, args...).Find(&lst).Error
	if err == nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	lst[0].DB2FE()
	return lst[0], nil
}

func RecordRuleGetById(id int64) (*AlertRule, error) {
	return AlertRuleGet("id=?", id)
}

func RecordRuleStatistics(cluster string) (*Statistics, error) {
	session := DB().Model(&RecordRule{}).Select("count(*) as total", "max(update_at) as last_updated").Where("disabled = ?", 0)
	if cluster != "" {
		session = session.Where("cluster = ?", cluster)
	}

	var stats []*Statistics
	err := session.Find(&stats).Error
	if err != nil {
		return nil, err
	}

	return stats[0], nil
}
