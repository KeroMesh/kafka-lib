package produce

import (
    "io"
    "bytes"
    "github.com/keromesh/kafka-lib/internal/wire"
    . "github.com/keromesh/kafka-lib/pkg/message/recordset"
)


/////////////////////////////////////////////////////////////////////
// ProduceRequest (key: 0, version: 8)
/////////////////////////////////////////////////////////////////////

type ProduceRequest struct {
    TransactionalId *string
    Acks int16
    Timeout int32
    TopicData []TopicData
}

type TopicData struct {
    Topic string
    Data []Data
}

type Data struct {
    Partition int32
    RecordSet *RecordSet
}

func (msg *ProduceRequest) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if apiVer > 2 {
        if msg.TransactionalId, err = wire.ReadNullableString(r); err != nil {
            return err
        }
    }
    if msg.Acks, err = wire.ReadInt16(r); err != nil {
        return err
    }
    if msg.Timeout, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.TopicData = make([]TopicData, len)
    for i := range msg.TopicData {
        if msg.TopicData[i].Topic, err = wire.ReadString(r); err != nil {
            return err
        }
        if len, err = wire.ReadInt32(r); err != nil {
            return err
        }
        msg.TopicData[i].Data = make([]Data, len)
        for j := range msg.TopicData[i].Data {
            if msg.TopicData[i].Data[j].Partition, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if len, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if len == 0 || len == -1 {
                msg.TopicData[i].Data[j].RecordSet = nil
            } else {
                msg.TopicData[i].Data[j].RecordSet = &RecordSet{}
                if err = msg.TopicData[i].Data[j].RecordSet.Decode(r, apiVer); err != nil {
                    return err
                }
            }
        }
    }

    return nil
}

func (msg *ProduceRequest) Encode(w io.Writer, apiVer int16) error {
    var err error

    if apiVer > 2 {
        if err = wire.WriteNullableString(w, msg.TransactionalId); err != nil {
            return err
        }
    }
    if err = wire.WriteInt16(w, msg.Acks); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, msg.Timeout); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, int32(len(msg.TopicData))); err != nil {
        return err
    }
    for i := range msg.TopicData {
        if err = wire.WriteString(w, msg.TopicData[i].Topic); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, int32(len(msg.TopicData[i].Data))); err != nil {
            return err
        }
        for j := range msg.TopicData[i].Data {
            if err = wire.WriteInt32(w, msg.TopicData[i].Data[j].Partition); err != nil {
                return err
            }
            if msg.TopicData[i].Data[j].RecordSet == nil {
                if err = wire.WriteInt32(w, 0); err != nil {
                    return err
                }
            } else {
                buff := bytes.NewBuffer(make([]byte, 0))
                if err = msg.TopicData[i].Data[j].RecordSet.Encode(buff, apiVer); err != nil {
                    return err
                }

                body := buff.Bytes()
                if err = wire.WriteInt32(w, int32(len(body))); err != nil {
                    return err
                }
                if _, err = w.Write(body); err != nil {
                    return err
                }
            }
        }
    }

    return nil
}



/////////////////////////////////////////////////////////////////////
// ProduceResponse (key: 0, version: 8)
/////////////////////////////////////////////////////////////////////

type ProduceResponse struct {
    Responses []Response
    ThrottleTimeMs int32
}

type Response struct {
    Topic string
    PartitionResponses []PartitionResponse
}

type PartitionResponse struct {
    Partition int32
    ErrorCode int16
    BaseOffset int64
    LogAppendTime int64
    LogStartOffset int64
    RecordErrors []RecordError
    ErrorMessage *string
}

type RecordError struct {
    BatchIndex int32
    BatchIndexErrorMessage *string
}

func (msg *ProduceResponse) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.Responses = make([]Response, len)
    for i := range msg.Responses {
        if msg.Responses[i].Topic, err = wire.ReadString(r); err != nil {
            return err
        }
        if len, err = wire.ReadInt32(r); err != nil {
            return err
        }
        msg.Responses[i].PartitionResponses = make([]PartitionResponse, len)
        for j := range msg.Responses[i].PartitionResponses {
            if msg.Responses[i].PartitionResponses[j].Partition, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if msg.Responses[i].PartitionResponses[j].ErrorCode, err = wire.ReadInt16(r); err != nil {
                return err
            }
            if msg.Responses[i].PartitionResponses[j].BaseOffset, err = wire.ReadInt64(r); err != nil {
                return err
            }
            if apiVer > 1 {
                if msg.Responses[i].PartitionResponses[j].LogAppendTime, err = wire.ReadInt64(r); err != nil {
                    return err
                }
            }
            if apiVer > 4 {
                if msg.Responses[i].PartitionResponses[j].LogStartOffset, err = wire.ReadInt64(r); err != nil {
                    return err
                }
            }
            if apiVer > 7 {
                if len, err = wire.ReadInt32(r); err != nil {
                    return err
                }
                msg.Responses[i].PartitionResponses[j].RecordErrors = make([]RecordError, len)
                for k := range msg.Responses[i].PartitionResponses[j].RecordErrors {
                    if msg.Responses[i].PartitionResponses[j].RecordErrors[k].BatchIndex, err = wire.ReadInt32(r); err != nil {
                        return err
                    }
                    if msg.Responses[i].PartitionResponses[j].RecordErrors[k].BatchIndexErrorMessage, err = wire.ReadNullableString(r); err != nil {
                        return err
                    }
                }
                if msg.Responses[i].PartitionResponses[j].ErrorMessage, err = wire.ReadNullableString(r); err != nil {
                    return err
                }
            }
        }
    }
    if apiVer > 0 {
        if msg.ThrottleTimeMs, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }

    return nil
}

func (msg *ProduceResponse) Encode(w io.Writer, apiVer int16) error {
    var err error

    if err = wire.WriteInt32(w, int32(len(msg.Responses))); err != nil {
        return err
    }
    for i := range msg.Responses {
        if err = wire.WriteString(w, msg.Responses[i].Topic); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, int32(len(msg.Responses[i].PartitionResponses))); err != nil {
            return err
        }
        for j := range msg.Responses[i].PartitionResponses {
            if err = wire.WriteInt32(w, msg.Responses[i].PartitionResponses[j].Partition); err != nil {
                return err
            }
            if err = wire.WriteInt16(w, msg.Responses[i].PartitionResponses[j].ErrorCode); err != nil {
                return err
            }
            if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].BaseOffset); err != nil {
                return err
            }
            if apiVer > 1 {
                if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].LogAppendTime); err != nil {
                    return err
                }
            }
            if apiVer > 4 {
                if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].LogStartOffset); err != nil {
                    return err
                }
            }
            if apiVer > 7 {
                if err = wire.WriteInt32(w, int32(len(msg.Responses[i].PartitionResponses[j].RecordErrors))); err != nil {
                    return err
                }
                for k := range msg.Responses[i].PartitionResponses[j].RecordErrors {
                    if apiVer > 7 {
                        if err = wire.WriteInt32(w, msg.Responses[i].PartitionResponses[j].RecordErrors[k].BatchIndex); err != nil {
                            return err
                        }
                        if err = wire.WriteNullableString(w, msg.Responses[i].PartitionResponses[j].RecordErrors[k].BatchIndexErrorMessage); err != nil {
                            return err
                        }
                    }
                }
                if err = wire.WriteNullableString(w, msg.Responses[i].PartitionResponses[j].ErrorMessage); err != nil {
                    return err
                }
            }
        }
    }
    if apiVer > 0 {
        if err = wire.WriteInt32(w, msg.ThrottleTimeMs); err != nil {
            return err
        }
    }

    return nil
}

