package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/c12s/kuiper/internal/domain"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type PlacementEtcdStore struct {
	client *clientv3.Client
}

func NewPlacementEtcdStore(client *clientv3.Client) domain.PlacementStore {
	return PlacementEtcdStore{
		client: client,
	}
}

func (s PlacementEtcdStore) Place(ctx context.Context, config domain.Config, req *domain.PlacementTask) *domain.Error {
	dao := PlacementTaskDAO{
		Id:         req.Id(),
		Org:        string(config.Org()),
		Namespace:  config.Namespace(),
		Name:       config.Name(),
		Version:    config.Version(),
		Node:       string(req.Node()),
		Status:     req.Status(),
		AcceptedAt: req.AcceptedAtUnixSec(),
		ResolvedAt: req.ResolvedAtUnixSec(),
	}

	key := dao.Key(config.Type())
	value, err := dao.Marshal()
	if err != nil {
		return domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	_, err = s.client.KV.Put(ctx, key, value)
	if err != nil {
		return domain.NewError(domain.ErrTypeDb, err.Error())
	}
	return nil
}

func (s PlacementEtcdStore) ListByConfig(ctx context.Context, org domain.Org, namespace, name string, version, configType string) ([]domain.PlacementTask, *domain.Error) {
	key := PlacementTaskDAO{
		Org:       string(org),
		Namespace: namespace,
		Name:      name,
		Version:   version,
	}.KeyPrefixByConfig(configType)
	resp, err := s.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeDb, err.Error())
	}

	reqs := make([]domain.PlacementTask, 0, resp.Count)
	for _, kv := range resp.Kvs {
		dao, err := NewPlacementTaskDAO(kv.Value)
		if err != nil {
			log.Println(err)
			continue
		}
		reqs = append(reqs, *domain.NewPlacementTask(dao.Id, domain.Node(dao.Node), dao.Status, dao.AcceptedAt, dao.ResolvedAt))
	}

	return reqs, nil
}

func (s PlacementEtcdStore) UpdateStatus(ctx context.Context, org domain.Org, namespace, name string, version string, configType string, taskId string, status domain.PlacementTaskStatus) *domain.Error {
	key := PlacementTaskDAO{
		Id:        taskId,
		Org:       string(org),
		Namespace: namespace,
		Name:      name,
		Version:   version,
	}.Key(configType)
	resp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return domain.NewError(domain.ErrTypeDb, err.Error())
	}
	if len(resp.Kvs) == 0 {
		return domain.NewError(domain.ErrTypeNotFound, fmt.Sprintf("task (id=%s) not found", taskId))
	}

	dao, err := NewPlacementTaskDAO(resp.Kvs[0].Value)
	if err != nil {
		return domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	dao.Status = status
	dao.ResolvedAt = time.Now().Unix()

	value, err := dao.Marshal()
	if err != nil {
		return domain.NewError(domain.ErrTypeMarshalSS, err.Error())
	}

	_, err = s.client.Put(ctx, key, value)
	if err != nil {
		return domain.NewError(domain.ErrTypeDb, err.Error())
	}
	return nil
}

type PlacementTaskDAO struct {
	Id         string
	Org        string
	Namespace  string
	Name       string
	Version    string
	Node       string
	Status     domain.PlacementTaskStatus
	AcceptedAt int64
	ResolvedAt int64
}

func (dao PlacementTaskDAO) Key(configType string) string {
	return fmt.Sprintf("placements/%s/%s/%s/%s/%s/%s", configType, dao.Org, dao.Namespace, dao.Name, dao.Version, dao.Id)
}

func (dao PlacementTaskDAO) KeyPrefixByConfig(configType string) string {
	return fmt.Sprintf("placements/%s/%s/%s/%s/%s/", configType, dao.Org, dao.Namespace, dao.Name, dao.Version)
}

func (dao PlacementTaskDAO) Marshal() (string, error) {
	jsonBytes, err := json.Marshal(dao)
	return string(jsonBytes), err
}

func NewPlacementTaskDAO(marshalled []byte) (PlacementTaskDAO, error) {
	dao := &PlacementTaskDAO{}
	err := json.Unmarshal(marshalled, dao)
	if err != nil {
		return PlacementTaskDAO{}, err
	}
	return *dao, nil
}
