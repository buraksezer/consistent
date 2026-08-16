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
	"fmt"
	"testing"
)

// On the read path, building a key costs more than the call we want to measure.
// So the benchmarks read their keys from a pool. The pool is filled before the
// timer starts. Its size is a power of two, so the loop can mask the counter
// instead of dividing it.
const (
	benchKeyPoolSize = 4096
	benchKeyPoolMask = benchKeyPoolSize - 1
)

// Member counts for the calls that walk or copy the member map.
var benchMemberCounts = []int{8, 64, 512}

// LocateKey is O(1) in the member count. These two sizes are far apart, so both
// must give the same result. If they differ, the lookup walks the ring.
var benchLocateKeyMemberCounts = []int{8, 512}

// The cost of distributePartitions grows with PartitionCount. So the write path
// benchmarks vary it together with the member count.
var (
	benchWritePartitionCounts = []int{23, 271}
	benchWriteMemberCounts    = []int{8, 64}
)

// The compiler may drop a call if nobody uses its result. These sinks prevent
// that.
var (
	benchMemberSink  Member
	benchMembersSink []Member
	benchFloatSink   float64
	benchLoadsSink   map[string]float64
	benchIntSink     int
	benchErrSink     error
)

func benchConfig(partitionCount int) Config {
	cfg := newConfig()
	cfg.PartitionCount = partitionCount
	return cfg
}

func benchMembers(count int) []Member {
	members := make([]Member, count)
	for i := 0; i < count; i++ {
		members[i] = testMember(fmt.Sprintf("node%d.olric", i))
	}
	return members
}

func benchKeys() [][]byte {
	keys := make([][]byte, benchKeyPoolSize)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}
	return keys
}

// benchRing builds a ring with count members and the default test config.
func benchRing(count int) *Consistent {
	return New(benchMembers(count), newConfig())
}

// runWritePath runs fn for every partition count and member count pair.
func runWritePath(b *testing.B, fn func(b *testing.B, cfg Config, memberCount int)) {
	b.Helper()
	for _, partitionCount := range benchWritePartitionCounts {
		for _, memberCount := range benchWriteMemberCounts {
			name := fmt.Sprintf("partitions=%d/members=%d", partitionCount, memberCount)
			b.Run(name, func(b *testing.B) {
				fn(b, benchConfig(partitionCount), memberCount)
			})
		}
	}
}

// runMemberCounts runs fn for every member count. The ring is ready before fn
// starts.
func runMemberCounts(b *testing.B, counts []int, fn func(b *testing.B, c *Consistent)) {
	b.Helper()
	for _, memberCount := range counts {
		b.Run(fmt.Sprintf("members=%d", memberCount), func(b *testing.B) {
			fn(b, benchRing(memberCount))
		})
	}
}

// BenchmarkNew measures the setup cost. New calls add once per member, then
// distributePartitions once. Each iteration starts from an empty ring, so the
// cost per iteration stays the same.
func BenchmarkNew(b *testing.B) {
	runWritePath(b, func(b *testing.B, cfg Config, memberCount int) {
		members := benchMembers(memberCount)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchIntSink = len(New(members, cfg).partitions)
		}
	})
}

// BenchmarkAddRemove adds and removes one member on a ring that already holds
// memberCount members. The size stays between memberCount and memberCount+1.
// Remove never sees an empty ring, so both calls run distributePartitions.
//
// There is no separate benchmark for Add or Remove. Both change the ring. In a
// loop with only one of them, the ring would grow or shrink on every iteration,
// and ns/op would be useless. BenchmarkNew shows the cost of an insert.
func BenchmarkAddRemove(b *testing.B) {
	runWritePath(b, func(b *testing.B, cfg Config, memberCount int) {
		c := New(benchMembers(memberCount), cfg)

		// The names come from a small pool. This gives different ring
		// positions without an allocation per iteration.
		const namePoolSize = 64
		names := make([]testMember, namePoolSize)
		for i := range names {
			names[i] = testMember(fmt.Sprintf("churn%d.olric", i))
		}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			member := names[i%namePoolSize]
			c.Add(member)
			c.Remove(member.String())
		}
	})
}

// BenchmarkFindPartitionID measures one hash call. It takes no lock. Use it as
// the baseline for the benchmarks below.
func BenchmarkFindPartitionID(b *testing.B) {
	c := benchRing(8)
	keys := benchKeys()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchIntSink = c.FindPartitionID(keys[i&benchKeyPoolMask])
	}
}

// BenchmarkLocateKey measures one hash plus one map read under RLock.
func BenchmarkLocateKey(b *testing.B) {
	runMemberCounts(b, benchLocateKeyMemberCounts, func(b *testing.B, c *Consistent) {
		keys := benchKeys()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchMemberSink = c.LocateKey(keys[i&benchKeyPoolMask])
		}
	})
}

// BenchmarkGetPartitionOwner measures the same lookup without the hash.
func BenchmarkGetPartitionOwner(b *testing.B) {
	cfg := newConfig()
	c := New(benchMembers(8), cfg)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchMemberSink = c.GetPartitionOwner(i % cfg.PartitionCount)
	}
}

// BenchmarkAverageLoad measures a few arithmetic operations under RLock.
func BenchmarkAverageLoad(b *testing.B) {
	c := benchRing(8)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchFloatSink = c.AverageLoad()
	}
}

// BenchmarkGetClosestN varies the member count. getClosestN hashes and sorts
// all members on every call, so the cost grows with that number.
func BenchmarkGetClosestN(b *testing.B) {
	runMemberCounts(b, benchMemberCounts, func(b *testing.B, c *Consistent) {
		keys := benchKeys()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchMembersSink, benchErrSink = c.GetClosestN(keys[i&benchKeyPoolMask], 3)
		}
	})
}

// BenchmarkGetMembers measures the slice copy. Every call makes a new one.
func BenchmarkGetMembers(b *testing.B) {
	runMemberCounts(b, benchMemberCounts, func(b *testing.B, c *Consistent) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchMembersSink = c.GetMembers()
		}
	})
}

// BenchmarkLoadDistribution measures the map copy. Every call makes a new one.
//
// The loads map has one entry per member that owns a partition. So its size
// cannot pass PartitionCount. The default test config has 23 partitions. With
// that config, the numbers for 64 and 512 members would be equal. This
// benchmark uses more partitions than members instead. Now the member count
// sets the size of the copy.
func BenchmarkLoadDistribution(b *testing.B) {
	cfg := benchConfig(1031)
	for _, memberCount := range benchMemberCounts {
		b.Run(fmt.Sprintf("members=%d", memberCount), func(b *testing.B) {
			c := New(benchMembers(memberCount), cfg)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchLoadsSink = c.LoadDistribution()
			}
		})
	}
}

// BenchmarkLocateKeyParallel shows how the read path behaves under load. All
// readers share one RWMutex. Each goroutine keeps its own counter for the key
// pool, so the result shows the cost of the lock and nothing else.
//
// Run it with -cpu=1,2,4,8 to see the trend.
func BenchmarkLocateKeyParallel(b *testing.B) {
	c := benchRing(8)
	keys := benchKeys()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		var member Member
		for pb.Next() {
			member = c.LocateKey(keys[i&benchKeyPoolMask])
			i++
		}
		benchMemberSink = member
	})
}
