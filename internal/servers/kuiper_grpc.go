package servers

import (
	"context"

	"github.com/c12s/kuiper/internal/domain"
	"github.com/c12s/kuiper/internal/services"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/c12s/kuiper/pkg/api"
	quasarapi "github.com/c12s/quasar/proto"
)

type KuiperGrpcServer struct {
	api.UnimplementedKuiperServer
	standalone *services.StandaloneConfigService
	groups     *services.ConfigGroupService
}

func NewKuiperServer(standalone *services.StandaloneConfigService, groups *services.ConfigGroupService) api.KuiperServer {
	return &KuiperGrpcServer{
		standalone: standalone,
		groups:     groups,
	}
}

func (s *KuiperGrpcServer) PutStandaloneConfig(ctx context.Context, req *api.NewStandaloneConfig) (*api.StandaloneConfig, error) {
	paramSet := mapProtoParamSet(req.Name, req.ParamSet)
	config := domain.NewStandaloneConfig(domain.Org(req.Organization), req.Namespace, req.Version, *paramSet)
	var schema *quasarapi.ConfigSchemaDetails
	if req.Schema != nil {
		schema = &quasarapi.ConfigSchemaDetails{
			Organization: req.Organization,
			SchemaName:   req.Schema.Name,
			Version:      req.Schema.Version,
		}
	}

	config, err := s.standalone.Put(ctx, config, schema)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.StandaloneConfig{
		Organization: string(config.Org()),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSet:     mapParamSet(config.ParamSet()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) GetStandaloneConfig(ctx context.Context, req *api.ConfigId) (*api.StandaloneConfig, error) {
	config, err := s.standalone.Get(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.StandaloneConfig{
		Organization: string(config.Org()),
		Namespace:    config.Namespace(),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSet:     mapParamSet(config.ParamSet()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) ListStandaloneConfig(ctx context.Context, req *api.ListStandaloneConfigReq) (*api.ListStandaloneConfigResp, error) {
	configs, err := s.standalone.List(ctx, domain.Org(req.Organization), req.Namespace)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ListStandaloneConfigResp{
		Configurations: make([]*api.StandaloneConfig, 0),
	}
	for _, config := range configs {
		configProto := &api.StandaloneConfig{
			Organization: string(config.Org()),
			Namespace:    config.Namespace(),
			Name:         config.Name(),
			Version:      config.Version(),
			CreatedAt:    config.CreatedAtUTC().String(),
			ParamSet:     mapParamSet(config.ParamSet()),
		}
		resp.Configurations = append(resp.Configurations, configProto)
	}
	return resp, nil
}

func (s *KuiperGrpcServer) DeleteStandaloneConfig(ctx context.Context, req *api.ConfigId) (*api.StandaloneConfig, error) {
	config, err := s.standalone.Delete(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.StandaloneConfig{
		Organization: string(config.Org()),
		Namespace:    config.Namespace(),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSet:     mapParamSet(config.ParamSet()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) DiffStandaloneConfig(ctx context.Context, req *api.DiffReq) (*api.DiffStandaloneConfigResp, error) {
	diffs, err := s.standalone.Diff(ctx, domain.Org(req.Reference.Organization), req.Reference.Namespace, req.Reference.Name, req.Reference.Version, domain.Org(req.Diff.Organization), req.Diff.Namespace, req.Diff.Name, req.Diff.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.DiffStandaloneConfigResp{
		Diffs: make([]*api.Diff, 0),
	}
	for _, diff := range diffs {
		resp.Diffs = append(resp.Diffs, &api.Diff{Type: string(diff.Type()), Diff: diff.Diff()})
	}
	return resp, nil
}

func (s *KuiperGrpcServer) PlaceStandaloneConfig(ctx context.Context, req *api.PlaceReq) (*api.PlaceResp, error) {
	tasks, err := s.standalone.Place(ctx, domain.Org(req.Config.Organization), req.Config.Namespace, req.Config.Name, req.Config.Version, req.Strategy)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.PlaceResp{
		Tasks: mapTasks(tasks),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) ListPlacementTaskByStandaloneConfig(ctx context.Context, req *api.ConfigId) (*api.ListPlacementTaskResp, error) {
	tasks, err := s.standalone.ListPlacementTasks(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ListPlacementTaskResp{
		Tasks: mapTasks(tasks),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) PutConfigGroup(ctx context.Context, req *api.NewConfigGroup) (*api.ConfigGroup, error) {
	paramSets := mapProtoParamSets(req.ParamSets)
	config := domain.NewConfigGroup(domain.Org(req.Organization), req.Namespace, req.Name, req.Version, paramSets)
	var schema *quasarapi.ConfigSchemaDetails
	if req.Schema != nil {
		schema = &quasarapi.ConfigSchemaDetails{
			Organization: req.Organization,
			Namespace:    req.Namespace,
			SchemaName:   req.Schema.Name,
			Version:      req.Schema.Version,
		}
	}

	config, err := s.groups.Put(ctx, config, schema)
	if err := mapError(err); err != nil {
		return nil, err
	}

	resp := &api.ConfigGroup{
		Organization: string(config.Org()),
		Namespace:    config.Namespace(),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSets:    mapParamSets(config.ParamSets()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) GetConfigGroup(ctx context.Context, req *api.ConfigId) (*api.ConfigGroup, error) {
	config, err := s.groups.Get(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ConfigGroup{
		Organization: string(config.Org()),
		Namespace:    config.Namespace(),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSets:    mapParamSets(config.ParamSets()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) ListConfigGroup(ctx context.Context, req *api.ListConfigGroupReq) (*api.ListConfigGroupResp, error) {
	configs, err := s.groups.List(ctx, domain.Org(req.Organization), req.Namespace)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ListConfigGroupResp{
		Groups: make([]*api.ConfigGroup, 0),
	}
	for _, config := range configs {
		configProto := &api.ConfigGroup{
			Organization: string(config.Org()),
			Namespace:    config.Namespace(),
			Name:         config.Name(),
			Version:      config.Version(),
			CreatedAt:    config.CreatedAtUTC().String(),
			ParamSets:    mapParamSets(config.ParamSets()),
		}
		resp.Groups = append(resp.Groups, configProto)
	}
	return resp, nil
}

func (s *KuiperGrpcServer) DeleteConfigGroup(ctx context.Context, req *api.ConfigId) (*api.ConfigGroup, error) {
	config, err := s.groups.Delete(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ConfigGroup{
		Organization: string(config.Org()),
		Namespace:    config.Namespace(),
		Name:         config.Name(),
		Version:      config.Version(),
		CreatedAt:    config.CreatedAtUTC().String(),
		ParamSets:    mapParamSets(config.ParamSets()),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) DiffConfigGroup(ctx context.Context, req *api.DiffReq) (*api.DiffConfigGroupResp, error) {
	diffsByConfig, err := s.groups.Diff(ctx, domain.Org(req.Reference.Organization), req.Reference.Namespace, req.Reference.Name, req.Reference.Version, domain.Org(req.Diff.Organization), req.Diff.Namespace, req.Diff.Name, req.Diff.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.DiffConfigGroupResp{
		Diffs: make(map[string]*api.Diffs),
	}
	for config, diffs := range diffsByConfig {
		diffsProto := &api.Diffs{
			Diffs: make([]*api.Diff, 0),
		}
		for _, diff := range diffs {
			diffsProto.Diffs = append(diffsProto.Diffs, &api.Diff{Type: string(diff.Type()), Diff: diff.Diff()})
		}
		resp.Diffs[config] = diffsProto
	}
	return resp, nil
}

func (s *KuiperGrpcServer) PlaceConfigGroup(ctx context.Context, req *api.PlaceReq) (*api.PlaceResp, error) {
	tasks, err := s.groups.Place(ctx, domain.Org(req.Config.Organization), req.Config.Namespace, req.Config.Name, req.Config.Version, req.Strategy)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.PlaceResp{
		Tasks: mapTasks(tasks),
	}
	return resp, nil
}

func (s *KuiperGrpcServer) ListPlacementTaskByConfigGroup(ctx context.Context, req *api.ConfigId) (*api.ListPlacementTaskResp, error) {
	tasks, err := s.groups.ListPlacementTasks(ctx, domain.Org(req.Organization), req.Namespace, req.Name, req.Version)
	if err := mapError(err); err != nil {
		return nil, err
	}
	resp := &api.ListPlacementTaskResp{
		Tasks: mapTasks(tasks),
	}
	return resp, nil
}

func GetAuthInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok && len(md.Get("authz-token")) > 0 {
			ctx = context.WithValue(ctx, "authz-token", md.Get("authz-token")[0])
		}
		return handler(ctx, req)
	}
}

func mapError(err *domain.Error) error {
	if err == nil {
		return nil
	}
	switch err.ErrType() {
	case domain.ErrTypeDb:
		return status.Error(codes.Internal, err.Message())
	case domain.ErrTypeMarshalSS:
		return status.Error(codes.Internal, err.Message())
	case domain.ErrTypeNotFound:
		return status.Error(codes.NotFound, err.Message())
	case domain.ErrTypeVersionExists:
		return status.Error(codes.AlreadyExists, err.Message())
	case domain.ErrTypeUnauthorized:
		return status.Error(codes.PermissionDenied, err.Message())
	case domain.ErrTypeInternal:
		return status.Error(codes.Internal, err.Message())
	case domain.ErrTypeSchemaInvalid:
		return status.Error(codes.InvalidArgument, err.Message())
	default:
		return status.Error(codes.Unknown, err.Message())
	}
}

func mapProtoParamSet(name string, params []*api.Param) *domain.NamedParamSet {
	paramSet := make(map[string]string)
	for _, param := range params {
		paramSet[param.Key] = param.Value
	}
	return domain.NewParamSet(name, paramSet)
}

func mapProtoParamSets(params []*api.NamedParamSet) []domain.NamedParamSet {
	paramSets := make([]domain.NamedParamSet, 0)
	for _, paramSet := range params {
		paramSets = append(paramSets, *mapProtoParamSet(paramSet.Name, paramSet.ParamSet))
	}
	return paramSets
}

func mapParamSet(params map[string]string) []*api.Param {
	paramSet := make([]*api.Param, 0)
	for key, value := range params {
		paramSet = append(paramSet, &api.Param{Key: key, Value: value})
	}
	return paramSet
}

func mapParamSets(paramSets []domain.NamedParamSet) []*api.NamedParamSet {
	protoParamSets := make([]*api.NamedParamSet, 0)
	for _, paramSet := range paramSets {
		params := mapParamSet(paramSet.ParamSet())
		protoParamSets = append(protoParamSets, &api.NamedParamSet{Name: paramSet.Name(), ParamSet: params})
	}
	return protoParamSets
}

func mapTasks(tasks []domain.PlacementTask) []*api.PlacementTask {
	protoTasks := make([]*api.PlacementTask, 0)
	for _, task := range tasks {
		protoTasks = append(protoTasks, &api.PlacementTask{
			Id:         task.Id(),
			Node:       string(task.Node()),
			Status:     task.Status().String(),
			AcceptedAt: task.AcceptedAtUTC().String(),
			ResolvedAt: task.ResolveddAtUTC().String(),
		})
	}
	return protoTasks
}
