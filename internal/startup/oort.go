package startup

import (
	oortapi "github.com/c12s/oort/pkg/api"
)

func newOortAdministratorClient(address string) (*oortapi.AdministrationAsyncClient, error) {
	return oortapi.NewAdministrationAsyncClient(address)
}
