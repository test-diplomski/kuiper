package startup

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/c12s/kuiper/internal/services"
	"github.com/c12s/kuiper/internal/store"
	"github.com/gorilla/mux"

	"github.com/c12s/kuiper/internal/configs"
	"github.com/c12s/kuiper/internal/servers"
	"github.com/c12s/kuiper/pkg/api"
	meridian_api "github.com/c12s/meridian/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type app struct {
	config            *configs.Config
	grpcServer        *grpc.Server
	taskWebhooks      *http.Server
	shutdownProcesses []func()
}

func NewAppWithConfig(config *configs.Config) (*app, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	return &app{
		config:            config,
		shutdownProcesses: make([]func(), 0),
	}, nil
}

func (a *app) init() {
	etcdConn, err := NewEtcdConn(a.config.EtcdAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.shutdownProcesses = append(a.shutdownProcesses, func() {
		log.Println("closing etcd conn")
		etcdConn.Close()
	})

	magnetarClient, err := newMagnetarClient(a.config.MagnetarAddress())
	if err != nil {
		log.Fatalln(err)
	}

	quasarClient, err := newQuasarClient(a.config.QuasarAddress())
	if err != nil {
		log.Fatalln(err)
	}

	agentQueueClient, err := newAgentQueueClient(a.config.AgentQueueAddress())
	if err != nil {
		log.Fatalln(err)
	}

	administratorClient, err := newOortAdministratorClient(a.config.NatsAddress())
	if err != nil {
		log.Fatalln(err)
	}
	conn, err := grpc.NewClient(os.Getenv("MERIDIAN_ADDRESS"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln(err)
	}
	meridian := meridian_api.NewMeridianClient(conn)

	authzService := services.NewAuthZService(a.config.TokenKey())

	standaloneConfigStore := store.NewStandaloneConfigEtcdStore(etcdConn)
	configGroupStore := store.NewConfigGroupEtcdStore(etcdConn)
	placementStore := store.NewPlacementEtcdStore(etcdConn)

	placementService := services.NewPlacementStore(magnetarClient, agentQueueClient, administratorClient, authzService, placementStore, a.config.WebhookUrl())
	standaloneConfigService := services.NewStandaloneConfigService(administratorClient, authzService, standaloneConfigStore, placementService, quasarClient, meridian)
	configGroupService := services.NewConfigGroupService(administratorClient, authzService, configGroupStore, placementService, quasarClient)

	kuiperGrpcServer := servers.NewKuiperServer(standaloneConfigService, configGroupService)
	s := grpc.NewServer(grpc.UnaryInterceptor(servers.GetAuthInterceptor()))
	api.RegisterKuiperServer(s, kuiperGrpcServer)
	reflection.Register(s)
	a.grpcServer = s

	webhooks := servers.NewTaskWebshooks(placementService)
	router := mux.NewRouter()
	router.HandleFunc("/standalone", webhooks.UpdateStandaloneConfigTaskStatus).Methods("POST")
	router.HandleFunc("/groups", webhooks.UpdateConfigGroupTaskStatus).Methods("POST")
	a.taskWebhooks = &http.Server{
		Addr:    a.config.WebhooksAddress(),
		Handler: router,
	}
}

func (a *app) startGrpcServer() error {
	lis, err := net.Listen("tcp", a.config.ServerAddress())
	if err != nil {
		return err
	}
	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := a.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return nil
}

func (a *app) startWebhooks() {
	err := a.taskWebhooks.ListenAndServe()
	log.Println(err)
}

func (a *app) Start() error {
	a.init()
	go a.startWebhooks()
	return a.startGrpcServer()
}

func (a *app) GracefulStop() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := a.taskWebhooks.Shutdown(ctx)
	if err != nil {
		log.Println(err)
	}
	a.grpcServer.GracefulStop()
	for _, shudownProcess := range a.shutdownProcesses {
		shudownProcess()
	}
}
