package services

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/c12s/kuiper/internal/domain"
	"github.com/c12s/kuiper/pkg/api"
	meridian_api "github.com/c12s/meridian/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	quasarapi "github.com/c12s/quasar/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type StandaloneConfigService struct {
	administrator *oortapi.AdministrationAsyncClient
	authorizer    *AuthZService
	store         domain.StandaloneConfigStore
	placements    *PlacementService
	quasar        quasarapi.ConfigSchemaServiceClient
	meridian      meridian_api.MeridianClient
}

func NewStandaloneConfigService(administrator *oortapi.AdministrationAsyncClient, authorizer *AuthZService, store domain.StandaloneConfigStore, placements *PlacementService, quasar quasarapi.ConfigSchemaServiceClient, meridian meridian_api.MeridianClient) *StandaloneConfigService {
	return &StandaloneConfigService{
		administrator: administrator,
		authorizer:    authorizer,
		store:         store,
		placements:    placements,
		quasar:        quasar,
		meridian:      meridian,
	}
}

func (s *StandaloneConfigService) Put(ctx context.Context, config *domain.StandaloneConfig, schema *quasarapi.ConfigSchemaDetails) (*domain.StandaloneConfig, *domain.Error) {
	_, err := s.meridian.GetNamespace(ctx, &meridian_api.GetNamespaceReq{
		OrgId: string(config.Org()),
		Name:  config.Namespace(),
	})
	if err != nil {
		return nil, domain.NewError(domain.ErrTypeNotFound, "config namespace not found")
	}
	if !s.authorizer.Authorize(ctx, PermConfigPut, OortResNamespace, string(config.Org())+"/"+config.Namespace()) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigPut))
	}
	if schema != nil {
		schema.Namespace = config.Namespace()
		configMap := make(map[string]map[string]string)
		configMap[config.Name()] = make(map[string]string)
		for key, value := range config.ParamSet() {
			configMap[config.Name()][key] = value
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
	putErr := s.store.Put(ctx, config)
	if putErr != nil {
		return nil, putErr
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

func (s *StandaloneConfigService) Get(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.StandaloneConfig, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeStandalone, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.store.Get(ctx, org, namespace, name, version)
}

func (s *StandaloneConfigService) List(ctx context.Context, org domain.Org, namespace string) ([]*domain.StandaloneConfig, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResOrg, string(org)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.store.List(ctx, org, namespace)
}

func (s *StandaloneConfigService) Delete(ctx context.Context, org domain.Org, namespace, name, version string) (*domain.StandaloneConfig, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigPut, OortResConfig, OortConfigId(domain.ConfTypeStandalone, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigPut))
	}
	return s.store.Delete(ctx, org, name, namespace, version)
}

func (s *StandaloneConfigService) Diff(ctx context.Context, referenceOrg domain.Org, referenceNamespace, referenceName, referenceVersion string, diffOrg domain.Org, diffNamespace, diffName, diffVersion string) ([]domain.Diff, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeStandalone, string(referenceOrg), referenceNamespace, referenceName, referenceVersion)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeStandalone, string(diffOrg), diffNamespace, diffName, diffVersion)) {
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

func (s *StandaloneConfigService) Place(ctx context.Context, org domain.Org, namespace, name, version string, strategy *api.PlaceReq_Strategy) ([]domain.PlacementTask, *domain.Error) {
	config, err := s.store.Get(ctx, org, namespace, name, version)
	if err != nil {
		return nil, err
	}
	return s.placements.Place(ctx, config, strategy, func(taskId string) ([]byte, *domain.Error) {
		config := &api.StandaloneConfig{
			Organization: string(config.Org()),
			Namespace:    namespace,
			Name:         config.Name(),
			Version:      config.Version(),
			CreatedAt:    config.CreatedAtUTC().String(),
			ParamSet:     mapParamSet(config.ParamSet()),
		}
		configMarshalled, err := proto.Marshal(config)
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
		}
		cmd := &api.ApplyConfigCommand{
			TaskId:    taskId,
			Namespace: namespace,
			Config:    configMarshalled,
			Type:      "standalone",
			Strategy:  strategy.Name,
		}
		cmdMarshalled, err := proto.Marshal(cmd)
		if err != nil {
			return nil, domain.NewError(domain.ErrTypeMarshalSS, err.Error())
		}
		return cmdMarshalled, nil
	}, "/standalone")
}

func (s *StandaloneConfigService) ListPlacementTasks(ctx context.Context, org domain.Org, namespace, name, version string) ([]domain.PlacementTask, *domain.Error) {
	if !s.authorizer.Authorize(ctx, PermConfigGet, OortResConfig, OortConfigId(domain.ConfTypeStandalone, string(org), namespace, name, version)) {
		return nil, domain.NewError(domain.ErrTypeUnauthorized, fmt.Sprintf("Permission denied: %s", PermConfigGet))
	}
	return s.placements.List(ctx, org, namespace, name, version, domain.ConfTypeStandalone)
}

func mapParamSet(params map[string]string) []*api.Param {
	paramSet := make([]*api.Param, 0)
	for key, value := range params {
		paramSet = append(paramSet, &api.Param{Key: key, Value: value})
	}
	return paramSet
}
