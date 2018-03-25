// Package consistent provides a consistent hashing function with bounded loads.
// For more information about the underlying algorithm, please take a look at
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

var (
	ErrInsufficientHostCount = errors.New("insufficient host count")
	ErrMemberNotFound        = errors.New("member could not be found in circle")
)

type Member interface {
	Name() string
}

// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
// Hasher should minimize collisions (generating same hash for different byte slice)
// and while performance is also important fast functions are preferable (i.e.
// you can use FarmHash family).
type Hasher interface {
	Sum64([]byte) uint64
}

type Config struct {
	Hasher            Hasher
	PartitionCount    int
	ReplicationFactor int
	LoadFactor        float64
}

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	mu sync.RWMutex

	config     *Config
	hasher     Hasher
	sortedSet  []uint64
	members    map[string]*Member
	partitions map[int]*Member
	ring       map[uint64]*Member
}

// New creates a new Consistent object.
func New(members []Member, config *Config) *Consistent {
	c := &Consistent{
		members:        make(map[string]*Member),
		partitionCount: uint64(partitionCount),
		ring:           make(map[uint64]*Member),
	}
	for _, member := range members {
		c.add(member)
	}
	c.distributePartitions()
	return c
}

func (c *Consistent) distributeWithLoad(
	partID, idx int,
	avgLoad float64,
	partitions map[int]*Member,
	loads map[string]float64) {
	for {
		i := c.sortedSet[idx]
		member := c.ring[i]
		load := loads[member.Name()]
		if load+1 <= avgLoad {
			partitions[partID] = member
			loads[member.Name()]++
			return
		}
		idx++
		if idx >= len(c.members) {
			idx = 0
		}
	}
}

func (c *Consistent) distributePartitions() {
	avgLoad := float64(c.partitionCount/uint64(len(c.members))) * 1.25
	avgLoad = math.Ceil(avgLoad)
	loads := make(map[string]float64)
	partitions := make(map[int]*Member)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hasher.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		c.distributeWithLoad(int(partID), idx, avgLoad, partitions, loads)
	}
	c.partitions = partitions
}

func (c *Consistent) add(member Member) {
	for i := 0; i < replicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.Name(), i))
		h := c.hasher.Sum64(key)
		c.ring[h] = &member
		c.sortedSet = append(c.sortedSet, h)
	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// Storing member at this map is useful to find backup hosts of a partition.
	c.members[member.Name()] = &member
}

// Add adds a new member to the consistent hash circle.
func (c *Consistent) Add(member Member) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[member.Name()]; ok {
		// We have already have this. Quit immediately.
		return
	}
	c.add(member)
	c.distributePartitions()
}

func (c *Consistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
		}
	}
}

// Remove removes a member from the consistent hash circle.
func (c *Consistent) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[name]; !ok {
		// There is no member with that name. Quit immediately.
		return
	}

	for i := 0; i < replicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", name, i))
		h := c.hasher.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
	delete(c.members, name)
	if len(c.members) == 0 {
		// consistent hash ring is empty now. Reset the partition table.
		c.partitions = make(map[int]*Member)
		return
	}
	c.distributePartitions()
}

// FindPartitionID returns partition id for given key.
func (c *Consistent) FindPartitionID(key uint64) int {
	return int(key % c.partitionCount)
}

// LocateKey finds a home for given key. It returns partition ID and member's name respectively.
func (c *Consistent) LocateKey(key uint64) (int, Member) {
	partID := c.FindPartitionID(key)
	return partID, c.GetPartitionOwner(partID)
}

// GetPartitionOwner returns the owner of the given partition.
func (c *Consistent) GetPartitionOwner(partID int) Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	member, ok := c.partitions[partID]
	if !ok {
		return Member{}
	}
	// Create a thread-safe copy of member and return it.
	return *member
}

// GetPartitionBackups returns backup members to replicate a partition's data.
func (c *Consistent) GetPartitionBackups(partID, backupCount int) ([]Member, error) {
	res := []Member{}
	if backupCount > len(c.members)-1 {
		return res, ErrInsufficientHostCount
	}

	var ownerKey uint64
	owner := c.GetPartitionOwner(partID)
	keys := []uint64{}
	kmems := make(map[uint64]*Member)
	for name, member := range c.members {
		key := c.hasher.Sum64([]byte(name))
		if name == owner.Name() {
			ownerKey = key
		}
		keys = append(keys, key)
		kmems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the member
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			break
		}
		idx++
	}

	// Find backup members.
	for len(res) < backupCount {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, *kmems[key])
	}
	return res, nil
}
