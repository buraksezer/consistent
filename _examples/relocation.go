package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type Member string

func (m Member) Name() string {
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func main() {
	// Create a new consistent instance.
	members := []consistent.Member{}
	for i := 0; i < 8; i++ {
		member := Member(fmt.Sprintf("node%d.olricmq", i))
		members = append(members, member)
	}
	// Modify PartitionCount, ReplicationFactor and LoadFactor to increase or decrease
	// relocation ratio.
	cfg := &consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		LoadFactor:        1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(members, cfg)

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).Name()
	}

	// Add a new member
	m := Member(fmt.Sprintf("node%d.olricmq", 9))
	c.Add(m)

	// Get the new layout and compare with the previous
	var changed int
	for partID, member := range owners {
		owner := c.GetPartitionOwner(partID)
		if member != owner.Name() {
			changed++
			fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.Name(), member)
		}
	}
	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/cfg.PartitionCount)
}
