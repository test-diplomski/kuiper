package servers

import (
	"context"
	"io"
	"log"
	"net/http"

	"github.com/c12s/kuiper/internal/domain"
	"github.com/c12s/kuiper/internal/services"
	"github.com/c12s/kuiper/pkg/api"
	"google.golang.org/protobuf/proto"
)

type TaskWebhooks struct {
	placements *services.PlacementService
}

func NewTaskWebshooks(placements *services.PlacementService) *TaskWebhooks {
	return &TaskWebhooks{
		placements: placements,
	}
}

func (tw *TaskWebhooks) UpdateStandaloneConfigTaskStatus(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}

	reply := &api.ApplyConfigReply{}
	err = proto.Unmarshal(body, reply)
	if err != nil {
		log.Println(err)
		return
	}
	config := &api.StandaloneConfig{}
	err = proto.Unmarshal(reply.Cmd.Config, config)
	if err != nil {
		log.Println(err)
		return
	}

	status, mapped := mapStatus(reply.Status)
	if !mapped {
		log.Printf("could not map status %s", reply.Status)
		return
	}
	updateErr := tw.placements.UpdateStatus(context.Background(), domain.Org(config.Organization), config.Namespace, config.Name, config.Version, domain.ConfTypeStandalone, reply.Cmd.TaskId, status)
	if updateErr != nil {
		log.Println(updateErr)
	}
}

func (tw *TaskWebhooks) UpdateConfigGroupTaskStatus(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}

	reply := &api.ApplyConfigReply{}
	err = proto.Unmarshal(body, reply)
	if err != nil {
		log.Println(err)
		return
	}
	config := &api.ConfigGroup{}
	err = proto.Unmarshal(reply.Cmd.Config, config)
	if err != nil {
		log.Println(err)
		return
	}

	status, mapped := mapStatus(reply.Status)
	if !mapped {
		log.Printf("could not map status %s", reply.Status)
		return
	}
	updateErr := tw.placements.UpdateStatus(context.Background(), domain.Org(config.Organization), config.Namespace, config.Name, config.Version, domain.ConfTypeGroup, reply.Cmd.TaskId, status)
	if updateErr != nil {
		log.Println(updateErr)
	}
}

func mapStatus(protoStatus api.TaskStatus) (domain.PlacementTaskStatus, bool) {
	switch protoStatus {
	case api.TaskStatus_Placed:
		return domain.PlacementTaskStatusPlaced, true
	case api.TaskStatus_Failed:
		return domain.PlacementTaskStatusFailed, true
	default:
		return domain.PlacementTaskStatusFailed, false
	}
}
