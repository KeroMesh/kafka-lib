package wire


func SizeOfInt8() int32 {
    return 8 / 8
}

func SizeOfInt16() int32 {
    return 16 / 8
}

func SizeOfInt32() int32 {
    return 32 / 8
}

func SizeOfString(s string) int32 {
    return SizeOfInt16() + int32(len(s))
}

func SizeOfNullableString(s *string) int32 {
    if s == nil {
        return SizeOfInt16()
    } else {
        return SizeOfString(*s)
    }
}
