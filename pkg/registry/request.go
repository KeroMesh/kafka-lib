package registry

import (
    "github.com/keromesh/kafka-lib/pkg/message"
    "github.com/keromesh/kafka-lib/pkg/message/fetch"
    "github.com/keromesh/kafka-lib/pkg/message/generic"
    "github.com/keromesh/kafka-lib/pkg/message/metadata"
    "github.com/keromesh/kafka-lib/pkg/message/produce"
)


func Request(apiKey int16, size int32) message.Message {
    switch apiKey {
    case 0:
        return &produce.ProduceRequest{}
    case 1:
        return &fetch.FetchRequest{}
    case 3:
        return &metadata.MetadataRequest{}
    default:
        msg := &generic.Generic{}
        msg.Size = size
        return msg
    }
}
