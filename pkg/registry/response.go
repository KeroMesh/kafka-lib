package registry

import (
    "github.com/keromesh/kafka-lib/pkg/message"
    "github.com/keromesh/kafka-lib/pkg/message/fetch"
    "github.com/keromesh/kafka-lib/pkg/message/generic"
    "github.com/keromesh/kafka-lib/pkg/message/metadata"
    "github.com/keromesh/kafka-lib/pkg/message/produce"
)


func Response(apiKey int16, size int32) message.Message {
    switch apiKey {
    case 0:
        return &produce.ProduceResponse{}
    case 1:
        return &fetch.FetchResponse{}
    case 3:
        return &metadata.MetadataResponse{}
    default:
        msg := &generic.Generic{}
        msg.Size = size
        return msg
    }
}
