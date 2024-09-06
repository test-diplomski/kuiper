package startup

import (
	magnetarapi "github.com/c12s/magnetar/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newMagnetarClient(address string) (magnetarapi.MagnetarClient, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return magnetarapi.NewMagnetarClient(conn), nil
}
