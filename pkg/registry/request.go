package registry

import (
    "github.com/dfarr/kafka-lib/pkg/message"
)


func Request(apiKey int16, size int32) message.Message {
    switch apiKey {
    default:
        msg := &message.Generic{}
        msg.Size = size

        return msg
    }
}
