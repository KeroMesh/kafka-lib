package fetch

import (
    "io"
    "bytes"
    "github.com/keromesh/kafka-lib/internal/wire"
    . "github.com/keromesh/kafka-lib/pkg/message/recordset"
)


/////////////////////////////////////////////////////////////////////
// FetchRequest (key: 1, version: 11)
/////////////////////////////////////////////////////////////////////

type FetchRequest struct {
    ReplicaId int32
    MaxWaitTime int32
    MinBytes int32
    MaxBytes int32
    IsolationLevel int8
    SessionId int32
    SessionEpoch int32
    Topics []Topic
    ForgottenTopicsData []ForgottenTopicsData
    RackId string
}

type ForgottenTopicsData struct {
    Topic string
    Partitions int32
}

type Topic struct {
    Topic string
    Partitions []Partition
}

type Partition struct {
    Partition int32
    CurrentLeaderEpoch int32
    FetchOffset int64
    LogStartOffset int64
    PartitionMaxBytes int32
}

func (msg *FetchRequest) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if msg.ReplicaId, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if msg.MaxWaitTime, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if msg.MinBytes, err = wire.ReadInt32(r); err != nil {
        return err
    }
    if apiVer > 2 {
        if msg.MaxBytes, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if apiVer > 3 {
        if msg.IsolationLevel, err = wire.ReadInt8(r); err != nil {
            return err
        }
    }
    if apiVer > 6 {
        if msg.SessionId, err = wire.ReadInt32(r); err != nil {
            return err
        }
        if msg.SessionEpoch, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.Topics = make([]Topic, len)
    for i := range msg.Topics {
        if msg.Topics[i].Topic, err = wire.ReadString(r); err != nil {
            return err
        }
        if len, err = wire.ReadInt32(r); err != nil {
            return err
        }
        msg.Topics[i].Partitions = make([]Partition, len)
        for j := range msg.Topics[i].Partitions {
            if msg.Topics[i].Partitions[j].Partition, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if apiVer > 8 {
                if msg.Topics[i].Partitions[j].CurrentLeaderEpoch, err = wire.ReadInt32(r); err != nil {
                    return err
                }
            }
            if msg.Topics[i].Partitions[j].FetchOffset, err = wire.ReadInt64(r); err != nil {
                return err
            }
            if apiVer > 4 {
                if msg.Topics[i].Partitions[j].LogStartOffset, err = wire.ReadInt64(r); err != nil {
                    return err
                }
            }
            if msg.Topics[i].Partitions[j].PartitionMaxBytes, err = wire.ReadInt32(r); err != nil {
                return err
            }
        }
    }
    if apiVer > 6 {
        if len, err = wire.ReadInt32(r); err != nil {
            return err
        }
        msg.ForgottenTopicsData = make([]ForgottenTopicsData, len)
        for k := range msg.ForgottenTopicsData {
            if msg.ForgottenTopicsData[k].Topic, err = wire.ReadString(r); err != nil {
                return err
            }
            if msg.ForgottenTopicsData[k].Partitions, err = wire.ReadInt32(r); err != nil {
                return err
            }
        }
    }
    if apiVer > 10 {
        if msg.RackId, err = wire.ReadString(r); err != nil {
            return err
        }
    }

    return nil
}

func (msg *FetchRequest) Encode(w io.Writer, apiVer int16) error {
    var err error

    if err = wire.WriteInt32(w, msg.ReplicaId); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, msg.MaxWaitTime); err != nil {
        return err
    }
    if err = wire.WriteInt32(w, msg.MinBytes); err != nil {
        return err
    }
    if apiVer > 2 {
        if err = wire.WriteInt32(w, msg.MaxBytes); err != nil {
            return err
        }
    }
    if apiVer > 3 {
        if err = wire.WriteInt8(w, msg.IsolationLevel); err != nil {
            return err
        }
    }
    if apiVer > 6 {
        if err = wire.WriteInt32(w, msg.SessionId); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, msg.SessionEpoch); err != nil {
            return err
        }
    }
    if err = wire.WriteInt32(w, int32(len(msg.Topics))); err != nil {
        return err
    }
    for i := range msg.Topics {
        if err = wire.WriteString(w, msg.Topics[i].Topic); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, int32(len(msg.Topics[i].Partitions))); err != nil {
            return err
        }
        for j := range msg.Topics[i].Partitions {
            if err = wire.WriteInt32(w, msg.Topics[i].Partitions[j].Partition); err != nil {
                return err
            }
            if apiVer > 8 {
                if err = wire.WriteInt32(w, msg.Topics[i].Partitions[j].CurrentLeaderEpoch); err != nil {
                    return err
                }
            }
            if err = wire.WriteInt64(w, msg.Topics[i].Partitions[j].FetchOffset); err != nil {
                return err
            }
            if apiVer > 4 {
                if err = wire.WriteInt64(w, msg.Topics[i].Partitions[j].LogStartOffset); err != nil {
                    return err
                }
            }
            if err = wire.WriteInt32(w, msg.Topics[i].Partitions[j].PartitionMaxBytes); err != nil {
                return err
            }
        }
    }
    if apiVer > 6 {
        if err = wire.WriteInt32(w, int32(len(msg.ForgottenTopicsData))); err != nil {
            return err
        }
        for k := range msg.ForgottenTopicsData {
            if apiVer > 6 {
                if err = wire.WriteString(w, msg.ForgottenTopicsData[k].Topic); err != nil {
                    return err
                }
                if err = wire.WriteInt32(w, msg.ForgottenTopicsData[k].Partitions); err != nil {
                    return err
                }
            }
        }
    }
    if apiVer > 10 {
        if err = wire.WriteString(w, msg.RackId); err != nil {
            return err
        }
    }

    return nil
}



/////////////////////////////////////////////////////////////////////
// FetchResponse (key: 1, version: 11)
/////////////////////////////////////////////////////////////////////

type FetchResponse struct {
    ThrottleTimeMs int32
    ErrorCode int16
    SessionId int32
    Responses []Response
}

type Response struct {
    Topic string
    PartitionResponses []PartitionResponse
}

type PartitionResponse struct {
    PartitionHeader PartitionHeader
    RecordSet *RecordSet
}

type PartitionHeader struct {
    Partition int32
    ErrorCode int16
    HighWatermark int64
    LastStableOffset int64
    LogStartOffset int64
    AbortedTransactions []AbortedTransaction
    PreferredReadReplica int32
}

type AbortedTransaction struct {
    ProducerId int64
    FirstOffset int64
}

func (msg *FetchResponse) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if apiVer > 0 {
        if msg.ThrottleTimeMs, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if apiVer > 6 {
        if msg.ErrorCode, err = wire.ReadInt16(r); err != nil {
            return err
        }
        if msg.SessionId, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
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
            msg.Responses[i].PartitionResponses[j].PartitionHeader = PartitionHeader{}
            if msg.Responses[i].PartitionResponses[j].PartitionHeader.Partition, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if msg.Responses[i].PartitionResponses[j].PartitionHeader.ErrorCode, err = wire.ReadInt16(r); err != nil {
                return err
            }
            if msg.Responses[i].PartitionResponses[j].PartitionHeader.HighWatermark, err = wire.ReadInt64(r); err != nil {
                return err
            }
            if apiVer > 3 {
                if msg.Responses[i].PartitionResponses[j].PartitionHeader.LastStableOffset, err = wire.ReadInt64(r); err != nil {
                    return err
                }
            }
            if apiVer > 4 {
                if msg.Responses[i].PartitionResponses[j].PartitionHeader.LogStartOffset, err = wire.ReadInt64(r); err != nil {
                    return err
                }
            }
            if apiVer > 3 {
                if len, err = wire.ReadInt32(r); err != nil {
                    return err
                }
                msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions = make([]AbortedTransaction, len)
                for k := range msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions {
                    if msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions[k].ProducerId, err = wire.ReadInt64(r); err != nil {
                        return err
                    }
                    if msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions[k].FirstOffset, err = wire.ReadInt64(r); err != nil {
                        return err
                    }
                }
            }
            if apiVer > 10 {
                if msg.Responses[i].PartitionResponses[j].PartitionHeader.PreferredReadReplica, err = wire.ReadInt32(r); err != nil {
                    return err
                }
            }
            if len, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if len == 0 || len == -1 {
                msg.Responses[i].PartitionResponses[j].RecordSet = nil
            } else {
                msg.Responses[i].PartitionResponses[j].RecordSet = &RecordSet{}
                if err = msg.Responses[i].PartitionResponses[j].RecordSet.Decode(r, apiVer); err != nil {
                    return err
                }
            }
        }
    }

    return nil
}

func (msg *FetchResponse) Encode(w io.Writer, apiVer int16) error {
    var err error

    if apiVer > 0 {
        if err = wire.WriteInt32(w, msg.ThrottleTimeMs); err != nil {
            return err
        }
    }
    if apiVer > 6 {
        if err = wire.WriteInt16(w, msg.ErrorCode); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, msg.SessionId); err != nil {
            return err
        }
    }
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
            if err = wire.WriteInt32(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.Partition); err != nil {
                return err
            }
            if err = wire.WriteInt16(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.ErrorCode); err != nil {
                return err
            }
            if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.HighWatermark); err != nil {
                return err
            }
            if apiVer > 3 {
                if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.LastStableOffset); err != nil {
                    return err
                }
            }
            if apiVer > 4 {
                if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.LogStartOffset); err != nil {
                    return err
                }
            }
            if apiVer > 3 {
                if err = wire.WriteInt32(w, int32(len(msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions))); err != nil {
                    return err
                }
                for k := range msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions {
                    if apiVer > 3 {
                        if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions[k].ProducerId); err != nil {
                            return err
                        }
                        if err = wire.WriteInt64(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.AbortedTransactions[k].FirstOffset); err != nil {
                            return err
                        }
                    }
                }
            }
            if apiVer > 10 {
                if err = wire.WriteInt32(w, msg.Responses[i].PartitionResponses[j].PartitionHeader.PreferredReadReplica); err != nil {
                    return err
                }
            }
            if msg.Responses[i].PartitionResponses[j].RecordSet == nil {
                if err = wire.WriteInt32(w, 0); err != nil {
                    return err
                }
            } else {
                buff := bytes.NewBuffer(make([]byte, 0))
                if err = msg.Responses[i].PartitionResponses[j].RecordSet.Encode(buff, apiVer); err != nil {
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

