package skyffel_kafka

import "fmt"

func murmur2Hash(partitionKey string, numPartitions int32) (int32, error) {

	if partitionKey == "" {
		return 0, fmt.Errorf("No key found to partition on")
	}

	data := []byte(partitionKey)
	length := len(data)
	const (
		seed uint32 = 0x9747b28c
		m           = 0x5bd1e995
		r           = 24
	)

	h := seed ^ uint32(length)
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := (uint32(data[i4+0]) & 0xff) + ((uint32(data[i4+1]) & 0xff) << 8) + ((uint32(data[i4+2]) & 0xff) << 16) + ((uint32(data[i4+3]) & 0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	extra := length % 4
	if extra >= 3 {
		h ^= (uint32(data[(length & ^3)+2]) & 0xff) << 16
	}
	if extra >= 2 {
		h ^= (uint32(data[(length & ^3)+1]) & 0xff) << 8
	}
	if extra >= 1 {
		h ^= uint32(data[length & ^3]) & 0xff
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	h = h & 0x7fffffff

	return int32(h) % numPartitions, nil
}
