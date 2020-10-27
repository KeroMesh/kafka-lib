package wire

import (
    "io"
)

func ReadStringArray(r io.Reader) ([]string, error) {
    l, e := ReadInt32(r)
    if e != nil {
        return nil, e
    }

    a := make([]string, l)
    for i := 0; i < len(a); i++ {
        s, e := ReadString(r)
        if e != nil {
            return nil, e
        }
        a[i] = s
    }

    return a, nil
}
