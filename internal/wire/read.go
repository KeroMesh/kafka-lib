package wire

import (
    "io"
    "errors"
    "encoding/binary"
)


const (
    MaxVarintLen16 = 3
    MaxVarintLen32 = 5
    MaxVarintLen64 = 10
)

func ReadInt8(r io.Reader) (int8, error) {
    b := []byte{0}
    _, err := io.ReadFull(r, b)
    if err != nil {
        return 0, err
    }
    return int8(b[0]), nil
}

func ReadInt16(r io.Reader) (int16, error) {
    b := []byte{0, 0}
    _, err := io.ReadFull(r, b)
    if err != nil {
        return 0, err
    }
    return int16(binary.BigEndian.Uint16(b)), nil
}

func ReadInt32(r io.Reader) (int32, error) {
    b := []byte{0, 0, 0, 0}
    _, err := io.ReadFull(r, b)
    if err != nil {
        return 0, err
    }
    return int32(binary.BigEndian.Uint32(b)), nil
}

func ReadInt32Array(r io.Reader) ([]int32, error) {
    l, err := ReadInt32(r)
    if err != nil {
        return nil, err
    }
    a := make([]int32, l)
    for i := range a {
        if a[i], err = ReadInt32(r); err != nil {
            return nil, err
        }
    }
    return a, nil
}

func ReadInt64(r io.Reader) (int64, error) {
    b := []byte{0, 0, 0, 0, 0, 0, 0, 0}
    _, err := io.ReadFull(r, b)
    if err != nil {
        return 0, err
    }
    return int64(binary.BigEndian.Uint64(b)), nil
}

// func ReadVarint(r io.Reader) (int64, error) {
//     return binary.ReadVarint(bufio.NewReader(r))
// }

var overflow = errors.New("binary: varint overflows a 64-bit integer")

// ReadUvarint reads an encoded unsigned integer from r and returns it as a uint64.
func ReadUvarint(r io.Reader) (uint64, error) {
    var x uint64
    var s uint
    for i := 0; i < MaxVarintLen64; i++ {
        b := []byte{0}
        _, err := io.ReadFull(r, b)
        if err != nil {
            return x, err
        }
        if b[0] < 0x80 {
            if i == 9 && b[0] > 1 {
                return x, overflow
            }
            return x | uint64(b[0])<<s, nil
        }
        x |= uint64(b[0]&0x7f) << s
        s += 7
    }
    return x, overflow
}

// ReadVarint reads an encoded signed integer from r and returns it as an int64.
func ReadVarint(r io.Reader) (int64, error) {
    ux, err := ReadUvarint(r) // ok to continue in presence of error
    x := int64(ux >> 1)
    if ux&1 != 0 {
        x = ^x
    }
    return x, err
}

func ReadBool(r io.Reader) (bool, error) {
    i, err := ReadInt8(r)
    if err != nil {
        return false, err
    }
    return i != 0, nil
}

func ReadString(r io.Reader) (string, error) {
    s, err := ReadInt16(r)
    if err != nil {
        return "", err
    }
    b := make([]byte, s)
    _, err2 := io.ReadFull(r, b)
    if err2 != nil {
        return "", err2
    }
    return string(b), nil
}

func ReadVarString(r io.Reader) (string, error) {
    s, err := ReadVarint(r)
    if err != nil {
        return "", err
    }
    b := make([]byte, s)
    _, err2 := io.ReadFull(r, b)
    if err2 != nil {
        return "", err2
    }
    return string(b), nil
}

func ReadNullableString(r io.Reader) (*string, error) {
    s, err := ReadInt16(r)
    if s == -1 {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    if s == -1 {
        return nil, nil
    } else {
        b := make([]byte, s)
        _, err := io.ReadFull(r, b)
        if err != nil {
            return nil, err
        }
        s := string(b)
        return &s, nil
    }
}

func ReadBytes(r io.Reader) ([]byte, error) {
    s, err := ReadInt32(r)
    if s == -1 {
        return []byte{}, nil
    }
    if err != nil {
        return []byte{}, err
    }
    b := make([]byte, s)
    _, err = io.ReadFull(r, b)
    if err != nil {
        return []byte{}, err
    }
    return b, nil
}

func ReadVarBytes(r io.Reader) ([]byte, error) {
    s, err := ReadVarint(r)
    if s == -1 {
        return []byte{}, nil
    }
    if err != nil {
        return []byte{}, err
    }
    b := make([]byte, s)
    _, err = io.ReadFull(r, b)
    if err != nil {
        return []byte{}, err
    }
    return b, nil
}

func ReadNullableBytes(r io.Reader) ([]byte, error) {
    s, err := ReadInt32(r)
    if s == -1 {
        return []byte{}, nil
    }
    if err != nil {
        return []byte{}, err
    }
    b := make([]byte, s)
    _, err = io.ReadFull(r, b)
    if err != nil {
        return []byte{}, err
    }
    return b, nil
}

func ReadTagBuffer(r io.Reader) (map[string]interface{}, error) {
    tb := make(map[string]interface{})
    return tb, nil
}
