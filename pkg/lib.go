package kafkalib

import (
    "io"
)


func DecodeRequest(r io.Reader) (*Request, error) {
    req := &Request{}
    if err := req.Decode(r); err != nil {
        return nil, err
    }
    return req, nil
}

func DecodeResponse(r io.Reader, apiKey int16, apiVer int16) (*Response, error) {
    res := &Response{}
    if err := res.Decode(r, apiKey, apiVer); err != nil {
        return nil, err
    }
    return res, nil
}
