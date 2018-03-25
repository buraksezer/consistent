package main

import (
	"fmt"

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

func main() {
	members := []consistent.Member{}
	for i := 0; i < 10; i++ {
		member := Member(fmt.Sprintf("node%d.olricmq", i))
		members = append(members, member)
	}
	cfg := &consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		LoadFactor:        1.25,
		Hasher:            hasher{},
	}

	c := consistent.New(members, cfg)
	owners := make(map[string]int)
	backups := make(map[string]int)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owner := c.GetPartitionOwner(partID)
		owners[owner.Name()]++
		bks, _ := c.GetPartitionBackups(partID, 1)
		for _, backup := range bks {
			if backup.Name() == owner.Name() {
				panic("backup and owner could not be the same")
			}
			backups[backup.Name()]++
		}
	}
	fmt.Println("average load:", c.AverageLoad())
	fmt.Println("owners:", owners)
	fmt.Println("backups:", backups)
	fmt.Println(c.GetMembers())
}
