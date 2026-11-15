package primitives

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

// Ring models consistent-hash routing with virtual nodes. It is a teaching primitive,
// not a production partition coordinator or a substitute for Kafka partitions.
type Ring struct {
	points []uint64
	owners map[uint64]string
}

func hash(s string) uint64 { b := sha256.Sum256([]byte(s)); return binary.BigEndian.Uint64(b[:8]) }
func NewRing(nodes []string, replicas int) *Ring {
	if replicas < 1 {
		replicas = 1
	}
	r := &Ring{owners: map[uint64]string{}}
	for _, n := range nodes {
		for i := 0; i < replicas; i++ {
			h := hash(n + "#" + strconv.Itoa(i))
			r.points = append(r.points, h)
			r.owners[h] = n
		}
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
	return r
}
func (r *Ring) Owner(key string) string {
	if len(r.points) == 0 {
		return ""
	}
	h := hash(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	return r.owners[r.points[i%len(r.points)]]
}
