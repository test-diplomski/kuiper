protoc --proto_path=./ \
        --go_out=../ \
        --go_opt=paths=source_relative \
        --go_opt=Mkuiper_model.proto=github.com/c12s/kuiper/pkg/api \
        kuiper_model.proto

protoc  --proto_path=./ \
        --go_out=../ \
        --go_opt=paths=source_relative \
        --go_opt=Mkuiper.proto=github.com/c12s/kuiper/pkg/api \
        --go-grpc_out=../ \
        --go-grpc_opt=paths=source_relative \
        --go-grpc_opt=Mkuiper.proto=github.com/c12s/kuiper/pkg/api \
        -I=kuiper_model.proto \
        -I=../../../../magnetar/pkg/api/proto/ \
        kuiper.proto