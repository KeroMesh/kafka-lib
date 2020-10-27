package kafkalib

import (
    "io"
    // "fmt"
    "bytes"
    "github.com/dfarr/kafka-lib/pkg/message"
    "github.com/dfarr/kafka-lib/pkg/registry"
    "github.com/dfarr/kafka-lib/internal/wire"
)


type Response struct {
    Size   int32
    CorrId int32
    Body   message.Message
}

func (res *Response) Decode(r io.Reader, apiKey int16, apiVer int16) error {
    var err error

    if res.Size, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if res.CorrId, err = wire.ReadInt32(r); err != nil {
        return err
    }

    res.Body = registry.Response(apiKey, res.bodySize())
    if err = res.Body.Decode(r, apiVer); err != nil {
        return err
    }

    return nil
}

func (res *Response) Encode(w io.Writer, apiKey int16, apiVer int16) error {
    var err error

    buff := bytes.NewBuffer(make([]byte, 0))
    if err = res.Body.Encode(buff, apiVer); err != nil {
        return err
    }

    body := buff.Bytes()
    if err = wire.WriteInt32(w, res.fullSize(len(body))); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, res.CorrId); err != nil {
        return err
    }
    if _, err = w.Write(body); err != nil {
        return err
    }

    return nil
}

func (res *Response) bodySize() int32 {
    size := res.Size
    size -= wire.SizeOfInt32()
    return size
}

func (res *Response) fullSize(bodySize int) int32 {
    size := wire.SizeOfInt32()
    size += int32(bodySize)
    return size
}
