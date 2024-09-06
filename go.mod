module github.com/c12s/kuiper

go 1.22.3

require (
	github.com/c12s/magnetar v1.0.0
	github.com/c12s/meridian v1.0.0
	github.com/c12s/oort v1.0.0
	github.com/c12s/quasar v1.0.0
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/nats-io/nats.go v1.31.0
	go.etcd.io/etcd/client/v3 v3.5.13
	google.golang.org/grpc v1.65.0
	google.golang.org/protobuf v1.34.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.13 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.13 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

replace github.com/c12s/magnetar => ../magnetar

replace github.com/c12s/quasar => ../quasar

replace github.com/c12s/oort => ../oort

replace github.com/c12s/meridian => ../meridian
