package domain

import (
	"context"
	"fmt"
	"time"
)

const (
	ConfTypeStandalone = "standalone"
	ConfTypeGroup      = "groups"
)

type Node string

type Namespace string

type Org string

type Config interface {
	Org() Org
	Namespace() string
	Name() string
	Version() string
	CreatedAtUnixSec() int64
	CreatedAtUTC() time.Time
	Type() string
}

type ConfigBase struct {
	org       Org
	namespace string
	version   string
	createdAt int64
}

func (c *ConfigBase) Org() Org {
	return c.org
}

func (c *ConfigBase) Namespace() string {
	return c.namespace
}

func (c *ConfigBase) Version() string {
	return c.version
}

func (c *ConfigBase) SetCreatedAt(createdAt time.Time) {
	c.createdAt = createdAt.Unix()
}

func (c *ConfigBase) CreatedAtUnixSec() int64 {
	return c.createdAt
}

func (c *ConfigBase) CreatedAtUTC() time.Time {
	return time.Unix(c.createdAt, 0).UTC()
}

type NamedParamSet struct {
	name   string
	params map[string]string
}

func NewParamSet(name string, params map[string]string) *NamedParamSet {
	return &NamedParamSet{
		name:   name,
		params: params,
	}
}

func (ps NamedParamSet) Name() string {
	return ps.name
}

func (ps NamedParamSet) ParamSet() map[string]string {
	return ps.params
}

func (ps NamedParamSet) Diff(cmp NamedParamSet) []Diff {
	diffs := make([]Diff, 0)

	labelsNew := ps.params
	labelsLatest := cmp.params

	for key, labelN := range labelsNew {
		labelL, ok := labelsLatest[key]

		var newDiff Diff
		if !ok {
			newDiff = Addition{
				Value: labelN,
				Key:   key,
			}

			diffs = append(diffs, newDiff)
		} else if ok && labelN != labelL {
			newDiff = Replace{
				Key: key,
				New: labelN,
				Old: labelL,
			}

			diffs = append(diffs, newDiff)
		}

	}

	for key, labelL := range labelsLatest {
		if _, ok := labelsNew[key]; !ok {
			delDiff := Deletion{
				Key:   key,
				Value: labelL,
			}

			diffs = append(diffs, delDiff)
		}
	}

	return diffs
}

type StandaloneConfig struct {
	ConfigBase
	paramSet NamedParamSet
}

func InitStandaloneConfig(org Org, namespace, version string, createdAt int64, paramSet NamedParamSet) *StandaloneConfig {
	return &StandaloneConfig{
		ConfigBase: ConfigBase{
			org:       org,
			namespace: namespace,
			version:   version,
			createdAt: createdAt,
		},
		paramSet: paramSet,
	}
}

func NewStandaloneConfig(org Org, namespace string, version string, paramSet NamedParamSet) *StandaloneConfig {
	return &StandaloneConfig{
		ConfigBase: ConfigBase{
			org:       org,
			namespace: namespace,
			version:   version,
		},
		paramSet: paramSet,
	}
}

func (c *StandaloneConfig) Name() string {
	return c.paramSet.name
}

func (c *StandaloneConfig) ParamSet() map[string]string {
	return c.paramSet.params
}

func (c *StandaloneConfig) Diff(cmp *StandaloneConfig) []Diff {
	return c.paramSet.Diff(cmp.paramSet)
}

func (c *StandaloneConfig) Type() string {
	return ConfTypeStandalone
}

type ConfigGroup struct {
	ConfigBase
	name      string
	paramSets []NamedParamSet
}

func InitConfigGroup(org Org, namespace, name, version string, createdAt int64, paramSets []NamedParamSet) *ConfigGroup {
	return &ConfigGroup{
		ConfigBase: ConfigBase{
			org:       org,
			namespace: namespace,
			version:   version,
			createdAt: createdAt,
		},
		name:      name,
		paramSets: paramSets,
	}
}

func NewConfigGroup(org Org, namespace, name, version string, paramSets []NamedParamSet) *ConfigGroup {
	return &ConfigGroup{
		ConfigBase: ConfigBase{
			org:       org,
			namespace: namespace,
			version:   version,
		},
		name:      name,
		paramSets: paramSets,
	}
}

func (c *ConfigGroup) Name() string {
	return c.name
}

func (c *ConfigGroup) ParamSets() []NamedParamSet {
	return c.paramSets
}

func (c *ConfigGroup) ParamSet(name string) (NamedParamSet, *Error) {
	for _, ps := range c.paramSets {
		if ps.name == name {
			return ps, nil
		}
	}
	return NamedParamSet{}, NewError(ErrTypeNotFound, fmt.Sprintf("param set (name: %s) not found", name))
}

func (c *ConfigGroup) Diff(cmp *ConfigGroup) map[string][]Diff {
	diffs := make(map[string][]Diff)

	groupNew := c
	groupLatest := cmp
	for _, newParamSet := range groupNew.paramSets {
		latestParamSet, err := groupLatest.ParamSet(newParamSet.name)
		if err != nil {
			//addition of config in group
			for key, value := range newParamSet.params {
				newDiff := Addition{
					Key:   key,
					Value: value,
				}
				diffs[newParamSet.name] = append(diffs[newParamSet.name], newDiff)
			}
		} else {
			diffs[newParamSet.name] = newParamSet.Diff(latestParamSet)
		}
	}
	for _, latestParamSet := range groupLatest.paramSets {
		_, err := groupNew.ParamSet(latestParamSet.name)
		if err != nil {
			//deletion of config in group
			for key, value := range latestParamSet.params {
				newDiff := Deletion{
					Key:   key,
					Value: value,
				}
				diffs[latestParamSet.name] = append(diffs[latestParamSet.name], newDiff)
			}
		}
	}

	return diffs
}

func (c *ConfigGroup) Type() string {
	return ConfTypeGroup
}

type StandaloneConfigStore interface {
	Put(ctx context.Context, config *StandaloneConfig) *Error
	Get(ctx context.Context, org Org, namespace, name, version string) (*StandaloneConfig, *Error)
	List(ctx context.Context, org Org, namespace string) ([]*StandaloneConfig, *Error)
	Delete(ctx context.Context, org Org, namespace, name, version string) (*StandaloneConfig, *Error)
}

type ConfigGroupStore interface {
	Put(ctx context.Context, config *ConfigGroup) *Error
	Get(ctx context.Context, org Org, namespace, name, version string) (*ConfigGroup, *Error)
	List(ctx context.Context, org Org, namespace string) ([]*ConfigGroup, *Error)
	Delete(ctx context.Context, org Org, namespace, name, version string) (*ConfigGroup, *Error)
}
