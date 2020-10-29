package wire

import (
    "io"
    "encoding/binary"
)

func WriteBool(w io.Writer, b bool) error {
    if b {
        return WriteInt8(w, 1)
    } else {
        return WriteInt8(w, 0)
    }
}

func WriteInt8(w io.Writer, v int8) error {
    err := binary.Write(w, binary.BigEndian, v)
    return err
}

func WriteInt16(w io.Writer, v int16) error {
    err := binary.Write(w, binary.BigEndian, v)
    return err
}

func WriteInt32(w io.Writer, v int32) error {
    err := binary.Write(w, binary.BigEndian, v)
    return err
}

func WriteInt64(w io.Writer, v int64) error {
    err := binary.Write(w, binary.BigEndian, v)
    return err
}

func WriteInt32Array(w io.Writer, v []int32) error {
    e := WriteInt32(w, int32(len(v)))
    if e != nil {
        return e
    }
    for _, i := range v {
        e := WriteInt32(w, i)
        if e != nil {
            return e
        }
    }
    return nil
}

func WriteString(w io.Writer, v string) error {
    e := WriteInt16(w, int16(len(v)))
    if e != nil {
        return e
    }
    _, e = w.Write([]byte(v))
    if e != nil {
        return e
    }
    return nil
}

func WriteVarString(w io.Writer, v string) error {
    e := WriteVarint(w, int64(len(v)))
    if e != nil {
        return e
    }
    _, e = w.Write([]byte(v))
    if e != nil {
        return e
    }
    return nil
}

func WriteStringArray(w io.Writer, v []string) error {
    e := WriteInt32(w, int32(len(v)))
    if e != nil {
        return e
    }
    for _, i := range v {
        e := WriteString(w, i)
        if e != nil {
            return e
        }
    }
    return nil
}

func WriteNullableString(w io.Writer, v *string) error {
    if v == nil {
        return WriteInt16(w, -1)
    } else {
        return WriteString(w, *v)
    }
}

func WriteNullableBytes(w io.Writer, v []byte) error {
    if len(v) == 0 {
        return WriteInt16(w, -1)
    } else {
        return WriteBytes(w, v)
    }
}

func WriteBytes(w io.Writer, v []byte) error {
    e := WriteInt32(w, int32(len(v)))
    if e != nil {
        return e
    }
    _, e = w.Write(v)
    if e != nil {
        return e
    }
    return nil
}

func WriteVarBytes(w io.Writer, v []byte) error {
    var l int
    if len(v) == 0 {
        l = -1
    } else {
        l = len(v)
    }

    e := WriteVarint(w, int64(l))
    if e != nil {
        return e
    }
    _, e = w.Write(v)
    if e != nil {
        return e
    }
    return nil
}

func WriteVarint(w io.Writer, v int64) error {
    dumb := make([]byte, 1024)
    l := binary.PutVarint(dumb, v)
    _, e := w.Write(dumb[0:l])
    return e
}

func WriteTagBuffer(w io.Writer, tb map[string]interface{}) error {
    return nil
}
