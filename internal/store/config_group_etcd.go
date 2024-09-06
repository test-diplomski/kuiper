package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/c12s/kuiper/internal/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ConfigGroupEtcdStore struct {
	client *clientv3.Client
}

func NewConfigGroupEtcdStore(client *clientv3.Client) domain.ConfigGroupStore {
	return ConfigGroupEtcdStore{
		client: client,
	}
}

func (s ConfigGroupEtcdStore) Put(ctx context.Context, config *domain.ConfigGroup) *domain.Error {
	dao := ConfigGroupDAO{
		Org:       string(config.Org()),
		Namespace: config.Namespace(),
		Name:      config.Name(),
		Version:   config.Version(),
		CreatedAt: config.CreatedAtUnixSec(),
	}
	for _, ps := range config.ParamSets() {
		psDao := struct {
			Name     string
			ParamSet map[string]string
		}{
			Name:     ps.Name(),
			ParamSet: ps.ParamSet(),
		}
		dao.ParamsSets = append(dao.ParamsSets, psDao)
	}

	key := dao.Key()
	value, err := dao.Marshal()
	if err != nil {
		return domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	resp, err := s.client.KV.Txn(ctx).If(clientv3.CreateRevision(key)).Then(clientv3.OpPut(key, value)).Commit()
	if !resp.Succeeded {
		return domain.NewError(domain.ErrTypeVersionExists, fmt.Sprintf("config group (Org: %s, name: %s, version: %s) already exists", config.Org(), config.Name(), config.Version()))
	}
	if err != nil {
		return domain.NewError(domain.ErrTypeDb, err.Error())
	}
	return nil
}

func (s ConfigGroupEtcdStore) Get(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.ConfigGroup, *domain.Error) {
	key := ConfigGroupDAO{
		Org:       string(org),
		Namespace: namespace,
		Name:      name,
		Version:   version,
	}.Key()
	resp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeDb, err.Error())
	}

	if resp.Count == 0 {
		return nil, domain.NewError(domain.ErrTypeNotFound, fmt.Sprintf("config group (Org: %s, name: %s, version: %s) not found", org, name, version))
	}

	dao, err := NewConfigGroupDAO(resp.Kvs[0].Value)
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	paramSets := make([]domain.NamedParamSet, 0, len(dao.ParamsSets))
	for _, psDao := range dao.ParamsSets {
		paramSets = append(paramSets, *domain.NewParamSet(psDao.Name, psDao.ParamSet))
	}

	return domain.InitConfigGroup(domain.Org(dao.Org), dao.Namespace, dao.Name, dao.Version, dao.CreatedAt, paramSets), nil
}

func (s ConfigGroupEtcdStore) List(ctx context.Context, org domain.Org, namespace string) ([]*domain.ConfigGroup, *domain.Error) {
	key := ConfigGroupDAO{
		Org:       string(org),
		Namespace: namespace,
	}.KeyPrefixAll()
	resp, err := s.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeDb, err.Error())
	}

	configs := make([]*domain.ConfigGroup, 0, resp.Count)
	for _, kv := range resp.Kvs {
		dao, err := NewConfigGroupDAO(kv.Value)
		if err != nil {
			log.Println(err)
			continue
		}

		paramSets := make([]domain.NamedParamSet, 0, len(dao.ParamsSets))
		for _, psDao := range dao.ParamsSets {
			paramSets = append(paramSets, *domain.NewParamSet(psDao.Name, psDao.ParamSet))
		}

		configs = append(configs, domain.InitConfigGroup(domain.Org(dao.Org), dao.Namespace, dao.Name, dao.Version, dao.CreatedAt, paramSets))
	}

	return configs, nil
}

func (s ConfigGroupEtcdStore) Delete(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.ConfigGroup, *domain.Error) {
	key := ConfigGroupDAO{
		Org:       string(org),
		Namespace: namespace,
		Name:      name,
		Version:   version,
	}.Key()
	resp, err := s.client.KV.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeDb, err.Error())
	}

	if len(resp.PrevKvs) == 0 {
		return nil, domain.NewError(domain.ErrTypeNotFound, fmt.Sprintf("config group (Org: %s, name: %s, version: %s) not found", org, name, version))
	}

	dao, err := NewConfigGroupDAO(resp.PrevKvs[0].Value)
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	paramSets := make([]domain.NamedParamSet, 0, len(dao.ParamsSets))
	for _, psDao := range dao.ParamsSets {
		paramSets = append(paramSets, *domain.NewParamSet(psDao.Name, psDao.ParamSet))
	}

	return domain.InitConfigGroup(domain.Org(dao.Org), dao.Namespace, dao.Name, dao.Version, dao.CreatedAt, paramSets), nil
}

type ConfigGroupDAO struct {
	Org        string
	Namespace  string
	Name       string
	Version    string
	CreatedAt  int64
	ParamsSets []struct {
		Name     string
		ParamSet map[string]string
	}
}

func (dao ConfigGroupDAO) Key() string {
	return fmt.Sprintf("groups/%s/%s/%s/%s", dao.Org, dao.Namespace, dao.Name, dao.Version)
}

func (dao ConfigGroupDAO) KeyPrefixAll() string {
	return fmt.Sprintf("groups/%s/%s/", dao.Org, dao.Namespace)
}

func (dao ConfigGroupDAO) Marshal() (string, error) {
	jsonBytes, err := json.Marshal(dao)
	return string(jsonBytes), err
}

func NewConfigGroupDAO(marshalled []byte) (ConfigGroupDAO, error) {
	dao := &ConfigGroupDAO{}
	err := json.Unmarshal(marshalled, dao)
	if err != nil {
		return ConfigGroupDAO{}, err
	}
	return *dao, nil
}
