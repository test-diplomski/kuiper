package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/c12s/kuiper/internal/domain"
	"github.com/c12s/kuiper/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	quasarapi "github.com/c12s/quasar/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type ConfigGroupService struct {
	administrator *oortapi.AdministrationAsyncClient
	authorizer    *AuthZService
	store         domain.ConfigGroupStore
	placements    *PlacementService
	quasar        quasarapi.ConfigSchemaServiceClient
}

func NewConfigGroupService(administrator *oortapi.AdministrationAsyncClient, authorizer *AuthZService, store domain.ConfigGroupStore, placements *PlacementService, quasar quasarapi.ConfigSchemaServiceClient) *ConfigGroupService {
	return &ConfigGroupService{
		administrator: administrator,
		authorizer:    authorizer,
		store:         store,
		placements:    placements,
		quasar:        quasar,
	}
}

func (s *ConfigGroupService) Put(ctx context.Context, config *domain.ConfigGroup, schema *quasarapi.ConfigSchemaDetails) (*domain.ConfigGroup, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigPut, OortResOrg, string(config.Org())) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigPut))
	}
	if schema != nil {
		configMap := make(map[string]map[string]string)
		for _, paramSet := range config.ParamSets() {
			configMap[paramSet.Name()] = make(map[string]string)
			for key, value := range paramSet.ParamSet() {
				configMap[paramSet.Name()][key] = value
			}
		}
		yamlBytes, err := yaml.Marshal(configMap)
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			log.Println("no metadata in ctx when sending req to magnetar")
		} else {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		resp, err := s.quasar.ValidateConfiguration(ctx, &quasarapi.ValidateConfigurationRequest{
			SchemaDetails: schema,
			Configuration: string(yamlBytes),
		})
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeInternal, err.Error())
		}
		if !resp.IsValid {
			return nil, domain.NewError(domain.ErrTypeSchemaInvalid, resp.Message)
		}
	}

	config.SetCreatedAt(time.Now())
	err := s.store.Put(ctx, config)
	if err != nil {
		return nil, err
	}
	err2 := s.administrator.SendRequest(&oortapi.CreateInheritanceRelReq{
		From: &oortapi.Resource{
			Id:   fmt.Sprintf("%s/%s", config.Org(), config.Namespace()),
			Kind: OortResNamespace,
		},
		To: &oortapi.Resource{
			Id:   OortConfigId(config.Type(), string(config.Org()), config.Namespace(), config.Name(), config.Version()),
			Kind: OortResConfig,
		},
	}, func(resp *oortapi.AdministrationAsyncResp) {
		log.Println(resp.Error)
	})
	if err2 != nil {
		log.Println(err2)
	}
	return config, nil
}

func (s *ConfigGroupService) Get(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.ConfigGroup, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeGroup, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.store.Get(ctx, org, namespace, name, version)
}

func (s *ConfigGroupService) List(ctx context.Context, org domain.Org, namespace string) ([]*domain.ConfigGroup, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResOrg, string(org)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.store.List(ctx, org, namespace)
}

func (s *ConfigGroupService) Delete(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.ConfigGroup, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigPut, OortResConfig, OortConfigId(domain.ConfTypeGroup, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigPut))
	}
	return s.store.Delete(ctx, org, namespace, name, version)
}

func (s *ConfigGroupService) Diff(ctx context.Context, referenceOrg domain.Org, referenceNamespace, referenceName, referenceVersion string, diffOrg domain.Org, diffNamespace, diffName, diffVersion string) (map[string][]domain.Diff, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeGroup, string(referenceOrg), referenceNamespace, referenceName, referenceVersion)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeGroup, string(diffOrg), diffNamespace, diffName, diffVersion)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	reference, err := s.store.Get(ctx, referenceOrg, referenceNamespace, referenceName, referenceVersion)
	if err != nil {
		return nil, err
	}
	diff, err := s.store.Get(ctx, diffOrg, diffNamespace, diffName, diffVersion)
	if err != nil {
		return nil, err
	}
	return diff.Diff(reference), nil
}

func (s *ConfigGroupService) Place(ctx context.Context, org domain.Org, namespace, name, version string, strategy *api.PlaceReq_Strategy) ([]domain.PlacementTask, *domain.Error) {
	config, err := s.store.Get(ctx, org, namespace, name, version)
	if err != nil {
		return nil, err
	}
	return s.placements.Place(ctx, config, strategy, func(taskId string) ([]byte, *domain.Error) {
		config := &api.ConfigGroup{
			Organization: string(config.Org()),
			Namespace:    config.Namespace(),
			Name:         config.Name(),
			Version:      config.Version(),
			CreatedAt:    config.CreatedAtUTC().String(),
			ParamSets:    mapParamSets(config.ParamSets()),
		}
		configMarshalled, err := proto.Marshal(config)
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
		}
		cmd := &api.ApplyConfigCommand{
			TaskId:    taskId,
			Namespace: namespace,
			Config:    configMarshalled,
			Type:      "group",
			Strategy:  strategy.Name,
		}
		cmdMarshalled, err := proto.Marshal(cmd)
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
		}
		return cmdMarshalled, nil
	}, "/groups")
}

func (s *ConfigGroupService) ListPlacementTasks(ctx context.Context, org domain.Org, namespace, name, version string) ([]domain.PlacementTask, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeGroup, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.placements.List(ctx, org, namespace, name, version, domain.ConfTypeGroup)
}

func mapParamSets(paramSets []domain.NamedParamSet) []*api.NamedParamSet {
	protoParamSets := make([]*api.NamedParamSet, 0)
	for _, paramSet := range paramSets {
		params := mapParamSet(paramSet.ParamSet())
		protoParamSets = append(protoParamSets, &api.NamedParamSet{Name: paramSet.Name(), ParamSet: params})
	}
	return protoParamSets
}
