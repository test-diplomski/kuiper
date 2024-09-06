package api

import "google.golang.org/protobuf/proto"

func (x *ApplyConfigCommand) Marshal() ([]byte, error) {
	return proto.Marshal(x)
}

func (x *ApplyConfigCommand) Unmarshal(cmdMarshalled []byte) error {
	return proto.Unmarshal(cmdMarshalled, x)
}
