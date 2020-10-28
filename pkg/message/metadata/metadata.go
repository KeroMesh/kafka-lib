package metadata

import (
    "io"
    "github.com/keromesh/kafka-lib/pkg/message"
    "github.com/keromesh/kafka-lib/internal/wire"
)


/////////////////////////////////////////////////////////////////////
// MetadataRequest (key: 3, version: 9)
/////////////////////////////////////////////////////////////////////

type MetadataRequest struct {
    Topics []RequestTopic
    AllowAutoTopicCreation bool
    IncludeClusterAuthorizedOperations bool
    IncludeTopicAuthorizedOperations bool
    TagBuffer message.TagBuffer
}

type RequestTopic struct {
    Name string
    TagBuffer message.TagBuffer
}

func (msg *MetadataRequest) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.Topics = make([]RequestTopic, len)
    for i := range msg.Topics {
        if msg.Topics[i].Name, err = wire.ReadString(r); err != nil {
            return err
        }
        if apiVer > 8 {
            if msg.Topics[i].TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
                return err
            }
        }
    }
    if apiVer > 3 {
        if msg.AllowAutoTopicCreation, err = wire.ReadBool(r); err != nil {
            return err
        }
    }
    if apiVer > 7 {
        if msg.IncludeClusterAuthorizedOperations, err = wire.ReadBool(r); err != nil {
            return err
        }
        if msg.IncludeTopicAuthorizedOperations, err = wire.ReadBool(r); err != nil {
            return err
        }
    }
    if apiVer > 8 {
        if msg.TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
            return err
        }
    }

    return nil
}

func (msg *MetadataRequest) Encode(w io.Writer, apiVer int16) error {
    var err error

    if err = wire.WriteInt32(w, int32(len(msg.Topics))); err != nil {
        return err
    }
    for i := range msg.Topics {
        if err = wire.WriteString(w, msg.Topics[i].Name); err != nil {
            return err
        }
        if apiVer > 8 {
            if err = wire.WriteTagBuffer(w, msg.Topics[i].TagBuffer); err != nil {
                return err
            }
        }
    }
    if apiVer > 3 {
        if err = wire.WriteBool(w, msg.AllowAutoTopicCreation); err != nil {
            return err
        }
    }
    if apiVer > 7 {
        if err = wire.WriteBool(w, msg.IncludeClusterAuthorizedOperations); err != nil {
            return err
        }
        if err = wire.WriteBool(w, msg.IncludeTopicAuthorizedOperations); err != nil {
            return err
        }
    }
    if apiVer > 8 {
        if err = wire.WriteTagBuffer(w, msg.TagBuffer); err != nil {
            return err
        }
    }

    return nil
}



/////////////////////////////////////////////////////////////////////
// MetadataResponse (key: 3, version: 9)
/////////////////////////////////////////////////////////////////////

type MetadataResponse struct {
    ThrottleTimeMs int32
    Brokers []Broker
    ClusterId *string
    ControllerId int32
    Topics []ResponseTopic
    ClusterAuthorizedOperations int32
    TagBuffer message.TagBuffer
}

type ResponseTopic struct {
    ErrorCode int16
    Name string
    IsInternal bool
    Partitions []Partition
    TopicAuthorizedOperations int32
    TagBuffer message.TagBuffer
}

type Partition struct {
    ErrorCode int16
    PartitionIndex int32
    LeaderId int32
    LeaderEpoch int32
    ReplicaNodes []int32
    IsrNodes []int32
    OfflineReplicas []int32
    TagBuffer message.TagBuffer
}

type Broker struct {
    NodeId int32
    Host string
    Port int32
    Rack *string
    TagBuffer message.TagBuffer
}

func (msg *MetadataResponse) Decode(r io.Reader, apiVer int16) error {
    var len int32
    var err error

    if apiVer > 2 {
        if msg.ThrottleTimeMs, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.Brokers = make([]Broker, len)
    for i := range msg.Brokers {
        if msg.Brokers[i].NodeId, err = wire.ReadInt32(r); err != nil {
            return err
        }
        if msg.Brokers[i].Host, err = wire.ReadString(r); err != nil {
            return err
        }
        if msg.Brokers[i].Port, err = wire.ReadInt32(r); err != nil {
            return err
        }
        if apiVer > 0 {
            if msg.Brokers[i].Rack, err = wire.ReadNullableString(r); err != nil {
                return err
            }
        }
        if apiVer > 8 {
            if msg.Brokers[i].TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
                return err
            }
        }
    }
    if apiVer > 1 {
        if msg.ClusterId, err = wire.ReadNullableString(r); err != nil {
            return err
        }
    }
    if apiVer > 0 {
        if msg.ControllerId, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if len, err = wire.ReadInt32(r); err != nil {
        return err
    }
    msg.Topics = make([]ResponseTopic, len)
    for j := range msg.Topics {
        if msg.Topics[j].ErrorCode, err = wire.ReadInt16(r); err != nil {
            return err
        }
        if msg.Topics[j].Name, err = wire.ReadString(r); err != nil {
            return err
        }
        if apiVer > 0 {
            if msg.Topics[j].IsInternal, err = wire.ReadBool(r); err != nil {
                return err
            }
        }
        if len, err = wire.ReadInt32(r); err != nil {
            return err
        }
        msg.Topics[j].Partitions = make([]Partition, len)
        for k := range msg.Topics[j].Partitions {
            if msg.Topics[j].Partitions[k].ErrorCode, err = wire.ReadInt16(r); err != nil {
                return err
            }
            if msg.Topics[j].Partitions[k].PartitionIndex, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if msg.Topics[j].Partitions[k].LeaderId, err = wire.ReadInt32(r); err != nil {
                return err
            }
            if apiVer > 6 {
                if msg.Topics[j].Partitions[k].LeaderEpoch, err = wire.ReadInt32(r); err != nil {
                    return err
                }
            }
            if msg.Topics[j].Partitions[k].ReplicaNodes, err = wire.ReadInt32Array(r); err != nil {
                return err
            }
            if msg.Topics[j].Partitions[k].IsrNodes, err = wire.ReadInt32Array(r); err != nil {
                return err
            }
            if apiVer > 4 {
                if msg.Topics[j].Partitions[k].OfflineReplicas, err = wire.ReadInt32Array(r); err != nil {
                    return err
                }
            }
            if apiVer > 8 {
                if msg.Topics[j].Partitions[k].TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
                    return err
                }
            }
        }
        if apiVer > 7 {
            if msg.Topics[j].TopicAuthorizedOperations, err = wire.ReadInt32(r); err != nil {
                return err
            }
        }
        if apiVer > 8 {
            if msg.Topics[j].TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
                return err
            }
        }
    }
    if apiVer > 7 {
        if msg.ClusterAuthorizedOperations, err = wire.ReadInt32(r); err != nil {
            return err
        }
    }
    if apiVer > 8 {
        if msg.TagBuffer, err = wire.ReadTagBuffer(r); err != nil {
            return err
        }
    }

    return nil
}

func (msg *MetadataResponse) Encode(w io.Writer, apiVer int16) error {
    var err error

    if apiVer > 2 {
        if err = wire.WriteInt32(w, msg.ThrottleTimeMs); err != nil {
            return err
        }
    }
    if err = wire.WriteInt32(w, int32(len(msg.Brokers))); err != nil {
        return err
    }
    for i := range msg.Brokers {
        if err = wire.WriteInt32(w, msg.Brokers[i].NodeId); err != nil {
            return err
        }
        if err = wire.WriteString(w, msg.Brokers[i].Host); err != nil {
            return err
        }
        if err = wire.WriteInt32(w, msg.Brokers[i].Port); err != nil {
            return err
        }
        if apiVer > 0 {
            if err = wire.WriteNullableString(w, msg.Brokers[i].Rack); err != nil {
                return err
            }
        }
        if apiVer > 8 {
            if err = wire.WriteTagBuffer(w, msg.Brokers[i].TagBuffer); err != nil {
                return err
            }
        }
    }
    if apiVer > 1 {
        if err = wire.WriteNullableString(w, msg.ClusterId); err != nil {
            return err
        }
    }
    if apiVer > 0 {
        if err = wire.WriteInt32(w, msg.ControllerId); err != nil {
            return err
        }
    }
    if err = wire.WriteInt32(w, int32(len(msg.Topics))); err != nil {
        return err
    }
    for j := range msg.Topics {
        if err = wire.WriteInt16(w, msg.Topics[j].ErrorCode); err != nil {
            return err
        }
        if err = wire.WriteString(w, msg.Topics[j].Name); err != nil {
            return err
        }
        if apiVer > 0 {
            if err = wire.WriteBool(w, msg.Topics[j].IsInternal); err != nil {
                return err
            }
        }
        if err = wire.WriteInt32(w, int32(len(msg.Topics[j].Partitions))); err != nil {
            return err
        }
        for k := range msg.Topics[j].Partitions {
            if err = wire.WriteInt16(w, msg.Topics[j].Partitions[k].ErrorCode); err != nil {
                return err
            }
            if err = wire.WriteInt32(w, msg.Topics[j].Partitions[k].PartitionIndex); err != nil {
                return err
            }
            if err = wire.WriteInt32(w, msg.Topics[j].Partitions[k].LeaderId); err != nil {
                return err
            }
            if apiVer > 6 {
                if err = wire.WriteInt32(w, msg.Topics[j].Partitions[k].LeaderEpoch); err != nil {
                    return err
                }
            }
            if err = wire.WriteInt32Array(w, msg.Topics[j].Partitions[k].ReplicaNodes); err != nil {
                return err
            }
            if err = wire.WriteInt32Array(w, msg.Topics[j].Partitions[k].IsrNodes); err != nil {
                return err
            }
            if apiVer > 4 {
                if err = wire.WriteInt32Array(w, msg.Topics[j].Partitions[k].OfflineReplicas); err != nil {
                    return err
                }
            }
            if apiVer > 8 {
                if err = wire.WriteTagBuffer(w, msg.Topics[j].Partitions[k].TagBuffer); err != nil {
                    return err
                }
            }
        }
        if apiVer > 7 {
            if err = wire.WriteInt32(w, msg.Topics[j].TopicAuthorizedOperations); err != nil {
                return err
            }
        }
        if apiVer > 8 {
            if err = wire.WriteTagBuffer(w, msg.Topics[j].TagBuffer); err != nil {
                return err
            }
        }
    }
    if apiVer > 7 {
        if err = wire.WriteInt32(w, msg.ClusterAuthorizedOperations); err != nil {
            return err
        }
    }
    if apiVer > 8 {
        if err = wire.WriteTagBuffer(w, msg.TagBuffer); err != nil {
            return err
        }
    }

    return nil
}

