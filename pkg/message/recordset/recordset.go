package produce

import (
    "io"
    "github.com/keromesh/kafka-lib/internal/wire"
)


/////////////////////////////////////////////////////////////////////
// RecordSet
/////////////////////////////////////////////////////////////////////

type RecordSet struct {
    BaseOffset int64
    BatchLength int32
    PartitionLeaderEpoch int32
    Magic int8
    Crc int32
    Attributes int16
    LastOffsetDelta int32
    FirstTimestamp int64
    MaxTimestamp int64
    ProducerId int64
    ProducerEpoch int16
    BaseSequence int32
    Records []Record
}

type Record struct {
    Length int64
    Attributes int8
    TimestampDelta int64
    OffsetDelta int64
    Key []byte
    Value []byte
    Headers []Header
}

type Header struct {
    Key string
    Value []byte
}

func (rs *RecordSet) Decode(r io.Reader, apiVer int16) error {
    var err error

    // RecordSet
    if rs.BaseOffset, err = wire.ReadInt64(r); err != nil {
        return err
    }
    if rs.BatchLength, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if rs.PartitionLeaderEpoch, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if rs.Magic, err = wire.ReadInt8(r); err != nil {
        return err
    }
    if rs.Crc, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if rs.Attributes, err = wire.ReadInt16(r); err != nil {
        return err
    }
    if rs.LastOffsetDelta, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if rs.FirstTimestamp, err = wire.ReadInt64(r); err != nil {
        return err
    }
    if rs.MaxTimestamp, err = wire.ReadInt64(r); err != nil {
        return err
    }
    if rs.ProducerId, err = wire.ReadInt64(r); err != nil {
        return err
    }
    if rs.ProducerEpoch, err = wire.ReadInt16(r); err != nil {
        return err
    }
    if rs.BaseSequence, err = wire.ReadInt32(r); err != nil {
        return err
    }

    records, err := wire.ReadInt32(r)
    if err != nil {
        return err
    }

    // Records
    rs.Records = make([]Record, records)
    for i := range rs.Records {
        if rs.Records[i].Length, err = wire.ReadVarint(r); err != nil {
            return err
        }
        if rs.Records[i].Attributes, err = wire.ReadInt8(r); err != nil {
            return err
        }
        if rs.Records[i].TimestampDelta, err = wire.ReadVarint(r); err != nil {
            return err
        }
        if rs.Records[i].OffsetDelta, err = wire.ReadVarint(r); err != nil {
            return err
        }
        if rs.Records[i].Key, err = wire.ReadVarBytes(r); err != nil {
            return err
        }
        if rs.Records[i].Value, err = wire.ReadVarBytes(r); err != nil {
            return err
        }

        headers, err := wire.ReadVarint(r)
        if err != nil {
            return err
        }

        // Headers
        rs.Records[i].Headers = make([]Header, headers)
        for j := range rs.Records[i].Headers {
            if rs.Records[i].Headers[j].Key, err = wire.ReadVarString(r); err != nil {
                return err
            }
            if rs.Records[i].Headers[j].Value, err = wire.ReadVarBytes(r); err != nil {
                return err
            }
        }
    }

    return nil
}

func (rs *RecordSet) Encode(w io.Writer, apiVer int16) error {
    var err error

    // RecordSet
    if err = wire.WriteInt64(w, rs.BaseOffset); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, rs.BatchLength); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, rs.PartitionLeaderEpoch); err != nil {
        return err
    }
    if err = wire.WriteInt8(w, rs.Magic); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, rs.Crc); err != nil {
        return err
    }
    if err = wire.WriteInt16(w, rs.Attributes); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, rs.LastOffsetDelta); err != nil {
        return err
    }
    if err = wire.WriteInt64(w, rs.FirstTimestamp); err != nil {
        return err
    }
    if err = wire.WriteInt64(w, rs.MaxTimestamp); err != nil {
        return err
    }
    if err = wire.WriteInt64(w, rs.ProducerId); err != nil {
        return err
    }
    if err = wire.WriteInt16(w, rs.ProducerEpoch); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, rs.BaseSequence); err != nil {
        return err
    }

    // Records
    if err = wire.WriteInt32(w, int32(len(rs.Records))); err != nil {
        return err
    }
    for i := range rs.Records {
        if err = wire.WriteVarint(w, rs.Records[i].Length); err != nil {
            return err
        }
        if err = wire.WriteInt8(w, rs.Records[i].Attributes); err != nil {
            return err
        }
        if err = wire.WriteVarint(w, rs.Records[i].TimestampDelta); err != nil {
            return err
        }
        if err = wire.WriteVarint(w, rs.Records[i].OffsetDelta); err != nil {
            return err
        }
        if err = wire.WriteVarBytes(w, rs.Records[i].Key); err != nil {
            return err
        }
        if err = wire.WriteVarBytes(w, rs.Records[i].Value); err != nil {
            return err
        }

        // Headers
        if err = wire.WriteVarint(w, int64(len(rs.Records[i].Headers))); err != nil {
            return err
        }
        for j := range rs.Records[i].Headers {
            if err = wire.WriteVarString(w, rs.Records[i].Headers[j].Key); err != nil {
                return err
            }
            if err = wire.WriteVarBytes(w, rs.Records[i].Headers[j].Value); err != nil {
                return err
            }
        }
    }

    return nil
}
