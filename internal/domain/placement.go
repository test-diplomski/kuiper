package domain

import (
	"context"
	"time"
)

type PlacementTaskStatus int8

const (
	PlacementTaskStatusAccepted PlacementTaskStatus = iota
	PlacementTaskStatusPlaced
	PlacementTaskStatusFailed
)

func (s PlacementTaskStatus) String() string {
	switch s {
	case PlacementTaskStatusAccepted:
		return "Accepted"
	case PlacementTaskStatusPlaced:
		return "Placed"
	case PlacementTaskStatusFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type PlacementTask struct {
	id         string
	node       Node
	status     PlacementTaskStatus
	acceptedAt int64
	resolvedAt int64
}

func NewPlacementTask(id string, node Node, status PlacementTaskStatus, acceptedAt, resolvedAt int64) *PlacementTask {
	return &PlacementTask{
		id:         id,
		node:       node,
		status:     status,
		acceptedAt: acceptedAt,
		resolvedAt: resolvedAt,
	}
}

func (p *PlacementTask) Id() string {
	return p.id
}

func (p *PlacementTask) Node() Node {
	return p.node
}

func (p *PlacementTask) AcceptedAtUnixSec() int64 {
	return p.acceptedAt
}

func (p *PlacementTask) AcceptedAtUTC() time.Time {
	return time.Unix(p.acceptedAt, 0).UTC()
}

func (p *PlacementTask) ResolvedAtUnixSec() int64 {
	return p.resolvedAt
}

func (p *PlacementTask) ResolveddAtUTC() time.Time {
	return time.Unix(p.resolvedAt, 0).UTC()
}

func (p *PlacementTask) Resolved() bool {
	return p.status != PlacementTaskStatusAccepted
}

func (p *PlacementTask) Status() PlacementTaskStatus {
	return p.status
}

type PlacementStore interface {
	Place(ctx context.Context, config Config, req *PlacementTask) *Error
	ListByConfig(ctx context.Context, org Org, namespace, name, version, configType string) ([]PlacementTask, *Error)
	UpdateStatus(ctx context.Context, org Org, namespace, name, version, configType, taskId string, status PlacementTaskStatus) *Error
}
