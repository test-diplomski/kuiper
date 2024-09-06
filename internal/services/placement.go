package services

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/c12s/kuiper/internal/domain"
	"github.com/c12s/kuiper/pkg/api"
	"github.com/c12s/kuiper/pkg/client/agent_queue"
	magnetarapi "github.com/c12s/magnetar/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

type PlacementService struct {
	magnetar       magnetarapi.MagnetarClient
	aq             agent_queue.AgentQueueClient
	administrator  *oortapi.AdministrationAsyncClient
	authorizer     *AuthZService
	store          domain.PlacementStore
	webhookBaseUrl string
}

func NewPlacementStore(magnetar magnetarapi.MagnetarClient, aq agent_queue.AgentQueueClient, administrator *oortapi.AdministrationAsyncClient, authorizer *AuthZService, store domain.PlacementStore, webhookBaseUrl string) *PlacementService {
	return &PlacementService{
		magnetar:       magnetar,
		aq:             aq,
		administrator:  administrator,
		authorizer:     authorizer,
		store:          store,
		webhookBaseUrl: webhookBaseUrl,
	}
}

func (s *PlacementService) Place(ctx context.Context, config domain.Config, strategy *api.PlaceReq_Strategy, cmd func(taskId string) ([]byte, *domain.Error), webhookPath string) ([]domain.PlacementTask, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(config.Type(), string(config.Org()), config.Namespace(), config.Name(), config.Version())) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	if !s.authorizer.Authorize(ctx, PermNsPut, OortResNamespace, fmt.Sprintf("%s/%s", config.Org(), config.Namespace())) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermNsPut))
	}

	var nodes []*magnetarapi.NodeStringified

	if strategy.Name == "default" {
		if strategy.Query == nil {
			return nil, domain.NewError(domain.ErrTypeSchemaInvalid, "Query is required for default strategy")
		}
		nodes, _ = s.placeByQuery(ctx, config, strategy.Query)
	} else if strategy.Name == "gossip" {
		if strategy.Percentage == 0 {
			return nil, domain.NewError(domain.ErrTypeSchemaInvalid, "Percentage can't be 0 for gossip strategy")
		}
		nodes, _ = s.placeByGossip(ctx, config, strategy.Percentage)
	} else {
		return nil, domain.NewError(domain.ErrTypeSchemaInvalid, fmt.Sprintf("Unknown strategy: %s", strategy.Name))
	}

	tasks := make([]domain.PlacementTask, 0)
	for _, node := range nodes {
		taskId := uuid.New().String()
		acceptedTs := time.Now().Unix()
		task := domain.NewPlacementTask(taskId, domain.Node(node.Id), domain.PlacementTaskStatusAccepted, acceptedTs, acceptedTs)
		placeErr := s.store.Place(ctx, config, task)
		if placeErr != nil {
			log.Println(placeErr)
			continue
		}
		tasks = append(tasks, *task)
		cmdMarshalled, err := cmd(taskId)
		if err != nil {
			log.Println(err)
			continue
		}
		deseminateErr := deseminateConfig(ctx, node.Id, cmdMarshalled, s.aq, s.webhookBaseUrl+webhookPath)
		if deseminateErr != nil {
			log.Println(deseminateErr)
		}
	}
	return tasks, nil
}

func (s *PlacementService) placeByQuery(ctx context.Context, config domain.Config, nodeQuery []*magnetarapi.Selector) ([]*magnetarapi.NodeStringified, *domain.Error) {
	queryReq := &magnetarapi.QueryOrgOwnedNodesReq{
		Org: string(config.Org()),
	}
	query := make([]*magnetarapi.Selector, 0)
	for _, selector := range nodeQuery {
		s := copySelector(selector)
		query = append(query, &s)
	}
	queryReq.Query = query
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Println("no metadata in ctx when sending req to magnetar")
	} else {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	queryResp, err := s.magnetar.QueryOrgOwnedNodes(ctx, queryReq)
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeInternal, err.Error())
	}
	return queryResp.Nodes, nil
}

func (s *PlacementService) placeByGossip(ctx context.Context, config domain.Config, percentage int32) ([]*magnetarapi.NodeStringified, *domain.Error) {
	queryReq := &magnetarapi.ListOrgOwnedNodesReq{
		Org: string(config.Org()),
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		log.Println("no metadata in ctx when sending req to magnetar")
	} else {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	queryResp, err := s.magnetar.ListOrgOwnedNodes(ctx, queryReq)
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeInternal, err.Error())
	}

	fmt.Printf("queryResp.Nodes: %+v\n", queryResp.Nodes)

	nodes := selectRandmNodes(queryResp.Nodes, percentage)
	return nodes, nil
}

func selectRandmNodes(nodes []*magnetarapi.NodeStringified, percentage int32) []*magnetarapi.NodeStringified {
	totalNodes := len(nodes)
	numberOfNodesToSelect := int(math.Ceil(float64(totalNodes) * float64(percentage) / 100))

	r := rand.New(rand.NewSource(time.Now().Unix()))

	selectedNodes := make([]*magnetarapi.NodeStringified, 0)

	for i := 0; i < numberOfNodesToSelect; i++ {
		index := r.Intn(len(nodes))
		selectedNodes = append(selectedNodes, nodes[index])
		nodes = append(nodes[:index], nodes[index+1:]...)
	}

	return selectedNodes
}

func (s *PlacementService) List(ctx context.Context, org domain.Org, namespace, name, version, configType string) ([]domain.PlacementTask, *domain.Error) {
	return s.store.ListByConfig(ctx, org, namespace, name, version, configType)
}

func (s *PlacementService) UpdateStatus(ctx context.Context, org domain.Org, namespace, name, version, configType, taskId string, status domain.PlacementTaskStatus) *domain.Error {
	return s.store.UpdateStatus(ctx, org, namespace, name, version, configType, taskId, status)
}

func deseminateConfig(ctx context.Context, nodeId string, cmd []byte, agentQueueClient agent_queue.AgentQueueClient, whUrl string) error {
	log.Printf("diseminating to node %s", nodeId)
	_, err := agentQueueClient.DeseminateConfig(ctx, &agent_queue.DeseminateConfigRequest{
		NodeId:  nodeId,
		Config:  cmd,
		Webhook: whUrl,
	})
	return err
}

func copySelector(selector *magnetarapi.Selector) magnetarapi.Selector {
	return magnetarapi.Selector{
		LabelKey: selector.LabelKey,
		ShouldBe: selector.ShouldBe,
		Value:    selector.Value,
	}
}
