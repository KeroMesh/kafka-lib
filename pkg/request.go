package kafkalib

import (
    "io"
    // "fmt"
    "github.com/dfarr/kafka-lib/pkg/message"
    "github.com/dfarr/kafka-lib/pkg/registry"
    "github.com/dfarr/kafka-lib/internal/wire"
)


type Request struct {
    Size   int32
    ApiKey int16
    ApiVer int16
    CorrId int32
    Client *string
    Body   message.Message
}

func (req *Request) Decode(r io.Reader) error {
    var err error

    if req.Size, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if req.ApiKey, err = wire.ReadInt16(r); err != nil {
        return err
    }
    if req.ApiVer, err = wire.ReadInt16(r); err != nil {
        return err
    }
    if req.CorrId, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if req.ApiVer > 0 {
        if req.Client, err = wire.ReadNullableString(r); err != nil {
            return err
        }
    }

    req.Body = registry.Request(req.ApiKey, req.bodySize())
    if err = req.Body.Decode(r, req.ApiVer); err != nil {
        return err
    }

    return nil
}

func (req *Request) Encode(w io.Writer) error {
    var err error

    if err = wire.WriteInt32(w, req.fullSize()); err != nil {
        return err
    }
    if err = wire.WriteInt16(w, req.ApiKey); err != nil {
        return err
    }
    if err = wire.WriteInt16(w, req.ApiVer); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, req.CorrId); err != nil {
        return err
    }
    if req.ApiVer > 0 {
        if err = wire.WriteNullableString(w, req.Client); err != nil {
            return err
        }
    }
    if err = req.Body.Encode(w, req.ApiVer); err != nil {
        return err
    }

    return nil
}

func (req *Request) bodySize() int32 {
    size := req.Size
    size -= wire.SizeOfInt16()
    size -= wire.SizeOfInt16()
    size -= wire.SizeOfInt32()
    if req.ApiVer > 1 {
        size -= wire.SizeOfNullableString(req.Client)
    }
    return size
}

func (req *Request) fullSize() int32 {
    size := wire.SizeOfInt16()
    size += wire.SizeOfInt16()
    size += wire.SizeOfInt32()
    size += req.bodySize()
    if req.ApiVer > 1 {
        size += wire.SizeOfNullableString(req.Client)
    }
    return size
}
