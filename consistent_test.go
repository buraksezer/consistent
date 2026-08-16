// Copyright (c) 2018-2026 Burak Sezer
// All rights reserved.
//
// This code is licensed under the MIT License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and / or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions :
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package consistent

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"testing"
)

func newConfig() Config {
	return Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type hasher struct{}

func (hs hasher) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func TestConsistentAdd(t *testing.T) {
	cfg := newConfig()
	c := New(nil, cfg)
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	for member := range members {
		found := false
		for _, mem := range c.GetMembers() {
			if member == mem.String() {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s could not be found", member)
		}
	}
}

func TestConsistentRemove(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	if len(c.GetMembers()) != len(members) {
		t.Fatalf("inserted member count is different")
	}
	for _, member := range members {
		c.Remove(member.String())
	}
	if len(c.GetMembers()) != 0 {
		t.Fatalf("member count should be zero")
	}
}

func TestConsistentLoad(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()

	t.Run("Average load should be greater than the member's load", func(t *testing.T) {
		c := New(members, cfg)
		if len(c.GetMembers()) != len(members) {
			t.Fatalf("inserted member count is different")
		}
		maxLoad := c.AverageLoad()
		for member, load := range c.LoadDistribution() {
			if load > maxLoad {
				t.Fatalf("%s exceeds max load. Its load: %f, max load: %f", member, load, maxLoad)
			}
		}
	})

	t.Run("Average load should equal to zero if there are no members", func(t *testing.T) {
		c := New(nil, cfg)
		if c.AverageLoad() != 0 {
			t.Fatalf("AverageLoad should equal to zero")
		}
	})
}

// Regression test for https://github.com/buraksezer/consistent/issues/16
//
// averageLoad divided two integers. This cut the fraction. The result was zero
// when the partition count was smaller than the member count. Then
// distributeWithLoad found no free space and panicked.
// See https://github.com/buraksezer/consistent/pull/29
func TestConsistentAverageLoadKeepsFraction(t *testing.T) {
	cases := []struct {
		partitionCount int
		memberCount    int
		load           float64
		// oldAvgLoad is the result of the old code.
		oldAvgLoad float64
		avgLoad    float64
	}{
		{partitionCount: 3, memberCount: 8, load: 1.25, oldAvgLoad: 0, avgLoad: 1},
		{partitionCount: 5, memberCount: 10, load: 1.25, oldAvgLoad: 0, avgLoad: 1},
		{partitionCount: 9, memberCount: 10, load: 1.25, oldAvgLoad: 0, avgLoad: 2},
		{partitionCount: 23, memberCount: 8, load: 1.25, oldAvgLoad: 3, avgLoad: 4},
	}

	for _, tc := range cases {
		name := fmt.Sprintf("%d partitions on %d members", tc.partitionCount, tc.memberCount)
		t.Run(name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Fatalf("Expected no panic, Got: %v", err)
				}
			}()

			var members []Member
			for i := 0; i < tc.memberCount; i++ {
				members = append(members, testMember(fmt.Sprintf("node%d.olric", i)))
			}
			cfg := Config{
				PartitionCount:    tc.partitionCount,
				ReplicationFactor: 20,
				Load:              tc.load,
				Hasher:            hasher{},
			}
			c := New(members, cfg)

			if got := c.AverageLoad(); got != tc.avgLoad {
				t.Fatalf("Expected average load %f, Got: %f", tc.avgLoad, got)
			}
			if tc.avgLoad == tc.oldAvgLoad {
				t.Fatalf("This case does not catch the bug. Both values are %f", tc.avgLoad)
			}

			// Every partition must have an owner.
			for partID := 0; partID < tc.partitionCount; partID++ {
				if c.GetPartitionOwner(partID) == nil {
					t.Fatalf("partition %d has no owner", partID)
				}
			}
		})
	}
}

// Regression test for https://github.com/buraksezer/consistent/issues/13
//
// distributeWithLoad increased the counter before it checked a position on the
// ring. So the last position was never checked. A ring with one position always
// panicked, even if the member was empty.
func TestConsistentDistributeWithLoadCoversWholeRing(t *testing.T) {
	newMembers := func(count int) []Member {
		var members []Member
		for i := 0; i < count; i++ {
			members = append(members, testMember(fmt.Sprintf("node%d.olric", i)))
		}
		return members
	}

	cases := []struct {
		name        string
		memberCount int
		config      Config
	}{
		{
			name:        "single member on a single ring position",
			memberCount: 1,
			config:      Config{PartitionCount: 100, ReplicationFactor: 1, Load: 1.25, Hasher: hasher{}},
		},
		{
			name:        "one partition on one member",
			memberCount: 1,
			config:      Config{PartitionCount: 1, ReplicationFactor: 1, Load: 1, Hasher: hasher{}},
		},
		{
			name:        "few partitions over few members without replicas",
			memberCount: 2,
			config:      Config{PartitionCount: 10, ReplicationFactor: 1, Load: 1.1, Hasher: hasher{}},
		},
		{
			name:        "default configuration",
			memberCount: 8,
			config:      Config{PartitionCount: 271, ReplicationFactor: 20, Load: 1.25, Hasher: hasher{}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Fatalf("Expected no panic, Got: %v", err)
				}
			}()

			c := New(newMembers(tc.memberCount), tc.config)
			// Every partition must have an owner.
			for partID := 0; partID < tc.config.PartitionCount; partID++ {
				if c.GetPartitionOwner(partID) == nil {
					t.Fatalf("partition %d has no owner", partID)
				}
			}
		})
	}
}

// The panic must still work after the fix above. If there is no room for all
// partitions, distributeWithLoad has to panic, not loop forever.
func TestConsistentDistributeWithLoadExhausted(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatal("Expected a panic, Got: nil")
		}
	}()

	var members []Member
	for i := 0; i < 2; i++ {
		members = append(members, testMember(fmt.Sprintf("node%d.olric", i)))
	}
	// Average load is Ceil(10/2 * 0.5) = 3. Two members can take 6 partitions,
	// but we have 10 of them.
	New(members, Config{PartitionCount: 10, ReplicationFactor: 1, Load: 0.5, Hasher: hasher{}})
}

// Regression test for https://github.com/buraksezer/consistent/issues/17
//
// The old code put the index after the member name. So "member" with index 10
// and "member1" with index 0 gave the same key: "member10". Both members used
// one position on the ring.
func TestConsistentReplicaKeyCollision(t *testing.T) {
	cfg := Config{
		PartitionCount:    23,
		ReplicationFactor: 11,
		Load:              1.25,
		Hasher:            hasher{},
	}
	members := []Member{testMember("member"), testMember("member1")}
	c := New(members, cfg)

	total := len(members) * cfg.ReplicationFactor
	if len(c.ring) != total {
		t.Fatalf("Expected %d positions on the ring, Got: %d", total, len(c.ring))
	}
	if len(c.sortedSet) != total {
		t.Fatalf("Expected %d hashes in sortedSet, Got: %d", total, len(c.sortedSet))
	}

	// Every hash on the ring must be unique.
	seen := make(map[uint64]struct{})
	for _, h := range c.sortedSet {
		if _, ok := seen[h]; ok {
			t.Fatalf("Duplicate hash on the ring: %d", h)
		}
		seen[h] = struct{}{}
	}

	// Remove one member. The other member must keep all of its positions.
	c.Remove("member1")
	if len(c.ring) != cfg.ReplicationFactor {
		t.Fatalf("Expected %d positions on the ring, Got: %d", cfg.ReplicationFactor, len(c.ring))
	}
	if len(c.sortedSet) != cfg.ReplicationFactor {
		t.Fatalf("Expected %d hashes in sortedSet, Got: %d", cfg.ReplicationFactor, len(c.sortedSet))
	}
	for h, member := range c.ring {
		if (*member).String() != "member" {
			t.Fatalf("Position %d belongs to %s, expected member", h, (*member).String())
		}
	}
}

func TestConsistentReplicaKey(t *testing.T) {
	// The key format is index, colon, member name.
	cases := []struct {
		name string
		idx  int
		key  string
	}{
		{name: "member", idx: 10, key: "10:member"},
		{name: "member1", idx: 0, key: "0:member1"},
		{name: "node0.olric", idx: 3, key: "3:node0.olric"},
	}
	for _, tc := range cases {
		if got := string(replicaKey(tc.name, tc.idx)); got != tc.key {
			t.Fatalf("Expected %s, Got: %s", tc.key, got)
		}
	}
}

func TestConsistentLocateKey(t *testing.T) {
	cfg := newConfig()
	c := New(nil, cfg)
	key := []byte("Olric")
	res := c.LocateKey(key)
	if res != nil {
		t.Fatalf("This should be nil: %v", res)
	}
	members := make(map[string]struct{})
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members[member.String()] = struct{}{}
		c.Add(member)
	}
	res = c.LocateKey(key)
	if res == nil {
		t.Fatalf("This shouldn't be nil: %v", res)
	}
}

func TestConsistentInsufficientMemberCount(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	key := []byte("Olric")
	_, err := c.GetClosestN(key, 30)
	if err != ErrInsufficientMemberCount {
		t.Fatalf("Expected ErrInsufficientMemberCount(%v), Got: %v", ErrInsufficientMemberCount, err)
	}
}

func TestConsistentClosestMembers(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		member := testMember(fmt.Sprintf("node%d.olric", i))
		members = append(members, member)
	}
	cfg := newConfig()
	c := New(members, cfg)
	key := []byte("Olric")
	closestn, err := c.GetClosestN(key, 2)
	if err != nil {
		t.Fatalf("Expected nil, Got: %v", err)
	}
	if len(closestn) != 2 {
		t.Fatalf("Expected closest member count is 2. Got: %d", len(closestn))
	}
	partID := c.FindPartitionID(key)
	owner := c.GetPartitionOwner(partID)
	for i, cl := range closestn {
		if i != 0 && cl.String() == owner.String() {
			t.Fatalf("Backup is equal the partition owner: %s", owner.String())
		}
	}
}

func TestConsistentNewWithEmptyMemberList(t *testing.T) {
	cases := []struct {
		name    string
		members []Member
	}{
		{name: "nil list", members: nil},
		{name: "empty list", members: []Member{}},
		{name: "empty list with capacity", members: make([]Member, 0, 4)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Fatalf("Expected no panic, Got: %v", err)
				}
			}()

			c := New(tc.members, newConfig())
			if len(c.GetMembers()) != 0 {
				t.Fatalf("Expected 0 members, Got: %d", len(c.GetMembers()))
			}
			if c.LocateKey([]byte("Olric")) != nil {
				t.Fatal("Expected no owner on an empty ring")
			}

			// The ring must still work after the first member arrives.
			c.Add(testMember("node0.olric"))
			if owner := c.LocateKey([]byte("Olric")); owner == nil {
				t.Fatal("Expected an owner after Add")
			}
		})
	}
}

func TestConsistentGetClosestNForPartitionOutOfRange(t *testing.T) {
	var members []Member
	for i := 0; i < 8; i++ {
		members = append(members, testMember(fmt.Sprintf("node%d.olric", i)))
	}
	cfg := newConfig()
	c := New(members, cfg)

	// The last valid id is PartitionCount - 1.
	badIDs := []int{-1, cfg.PartitionCount, cfg.PartitionCount + 1, 9999}
	for _, partID := range badIDs {
		t.Run(fmt.Sprintf("partition %d", partID), func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {
					t.Fatalf("Expected no panic, Got: %v", err)
				}
			}()

			// The count must not change the result.
			for _, count := range []int{0, 1, 2} {
				res, err := c.GetClosestNForPartition(partID, count)
				if !errors.Is(err, ErrPartitionNotFound) {
					t.Fatalf("Expected ErrPartitionNotFound(%v), Got: %v", ErrPartitionNotFound, err)
				}
				if len(res) != 0 {
					t.Fatalf("Expected an empty result, Got: %d members", len(res))
				}
			}
		})
	}

	// A valid id must still work.
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		res, err := c.GetClosestNForPartition(partID, 2)
		if err != nil {
			t.Fatalf("Expected nil, Got: %v", err)
		}
		if len(res) != 2 {
			t.Fatalf("Expected 2 members for partition %d, Got: %d", partID, len(res))
		}
	}
}

// All public methods must be safe for concurrent use. Run this test with -race.
func TestConsistentConcurrentAccess(t *testing.T) {
	const (
		writerCount = 4
		readerCount = 8
		iterations  = 150
	)

	// The seed member is never removed. So the ring is never empty.
	c := New([]Member{testMember("seed.olric")}, newConfig())

	var writers, readers sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < writerCount; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < iterations; i++ {
				name := fmt.Sprintf("w%d-node%d.olric", w, i%5)
				c.Add(testMember(name))

				// Only this goroutine owns this name. It must be there now.
				found := false
				for _, member := range c.GetMembers() {
					if member.String() == name {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("%s could not be found after Add", name)
					return
				}
				c.Remove(name)
			}
		}(w)
	}

	for r := 0; r < readerCount; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			key := []byte(fmt.Sprintf("key%d", r))
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.LocateKey(key)
				c.GetMembers()
				c.AverageLoad()
				c.LoadDistribution()
				c.GetPartitionOwner(c.FindPartitionID(key))
				_, _ = c.GetClosestN(key, 1)
			}
		}(r)
	}

	writers.Wait()
	close(stop)
	readers.Wait()

	// The writers removed all of their members. Only the seed is left.
	members := c.GetMembers()
	if len(members) != 1 {
		t.Fatalf("Expected 1 member, Got: %d", len(members))
	}
	if members[0].String() != "seed.olric" {
		t.Fatalf("Expected seed.olric, Got: %s", members[0].String())
	}
}
