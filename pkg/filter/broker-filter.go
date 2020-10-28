package filter

import (
    "fmt"
    "github.com/dfarr/kafka-lib/pkg"
    "github.com/dfarr/kafka-lib/pkg/message/metadata"
)


type BrokerFilter struct {
    Port int32
}

func (filter *BrokerFilter) Request(req *kafkalib.Request) error {
    return nil
}

func (filter *BrokerFilter) Response(res *kafkalib.Response) error {
    body := res.Body.(*metadata.MetadataResponse)
    for i := range body.Brokers {
        body.Brokers[i].Port = filter.Port
    }

    fmt.Println("After applying filter:", res.Body)

    return nil
}
