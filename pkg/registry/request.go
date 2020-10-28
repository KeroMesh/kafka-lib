package registry

import (
    "github.com/keromesh/kafka-lib/pkg/message"
    "github.com/keromesh/kafka-lib/pkg/message/generic"
    "github.com/keromesh/kafka-lib/pkg/message/metadata"
)


func Request(apiKey int16, size int32) message.Message {
    switch apiKey {
    case 3:
        return &metadata.MetadataRequest{}
    default:
        msg := &generic.Generic{}
        msg.Size = size
        return msg
    }
}
