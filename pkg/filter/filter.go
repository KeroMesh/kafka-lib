package filter

import (
    "github.com/keromesh/kafka-lib/pkg"
)


type Filter interface {
    Request(*kafkalib.Request) error
    Response(*kafkalib.Response) error
}
