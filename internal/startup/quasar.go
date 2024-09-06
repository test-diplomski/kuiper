package startup

import (
	quasarapi "github.com/c12s/quasar/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newQuasarClient(address string) (quasarapi.ConfigSchemaServiceClient, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return quasarapi.NewConfigSchemaServiceClient(conn), nil
}
