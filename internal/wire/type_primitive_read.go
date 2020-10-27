package wire

import (
    "bufio"
    "encoding/binary"
    "io"
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

func ReadInt64(r io.Reader) (int64, error) {
    b := []byte{0, 0, 0, 0, 0, 0, 0, 0}
    _, err := io.ReadFull(r, b)
    if err != nil {
        return 0, err
    }
    return int64(binary.BigEndian.Uint64(b)), nil
}

func ReadVarint(r io.Reader) (int64, error) {
    return binary.ReadVarint(bufio.NewReader(r))
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
