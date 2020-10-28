package registry

import (
    "github.com/dfarr/kafka-lib/pkg/message"
    "github.com/dfarr/kafka-lib/pkg/message/generic"
    "github.com/dfarr/kafka-lib/pkg/message/metadata"
)


func Response(apiKey int16, size int32) message.Message {
    switch apiKey {
    case 3:
        return &metadata.MetadataResponse{}
    default:
        msg := &generic.Generic{}
        msg.Size = size
        return msg
    }
}
