package startup

import (
	"github.com/c12s/kuiper/pkg/client/agent_queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newAgentQueueClient(address string) (agent_queue.AgentQueueClient, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return agent_queue.NewAgentQueueClient(conn), nil
}
