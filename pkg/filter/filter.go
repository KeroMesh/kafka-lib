package filter

import (
    "github.com/keromesh/kafka-lib/pkg"
)


type Filter interface {
    Request(*kafkalib.Request, int16) error
    Response(*kafkalib.Response, int16) error
}
