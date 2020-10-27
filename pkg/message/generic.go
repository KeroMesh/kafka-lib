package message

import (
    "io"
)


type Generic struct {
    Size   int32
    Bytes  []byte
}

func (msg *Generic) Decode(r io.Reader, apiVer int16) error {
    msg.Bytes = make([]byte, msg.Size)
    _, err := io.ReadFull(r, msg.Bytes)
    return err
}

func (msg *Generic) Encode(w io.Writer, apiVer int16) error {
    _, err := w.Write(msg.Bytes)
    return err
}

func (msg *Generic) GetSize() int32 {
    return 0
}
