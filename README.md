# Consistent

[![Go Reference](https://pkg.go.dev/badge/github.com/buraksezer/consistent.svg)](https://pkg.go.dev/github.com/buraksezer/consistent)
[![Build](https://github.com/buraksezer/consistent/actions/workflows/tests.yml/badge.svg)](https://github.com/buraksezer/consistent/actions/workflows/tests.yml) 
[![Release](https://img.shields.io/github/v/release/buraksezer/consistent)](https://github.com/buraksezer/consistent/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  


This library provides a consistent hashing function which simultaneously achieves both uniformity and consistency. 

For detailed information about the concept, you should take a look at the following resources:

* [Consistent Hashing with Bounded Loads on Google Research Blog](https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html)
* [Improving load balancing with a new consistent-hashing algorithm on Vimeo Engineering Blog](https://medium.com/vimeo-engineering-blog/improving-load-balancing-with-a-new-consistent-hashing-algorithm-9f1bd75709ed)
* [Consistent Hashing with Bounded Loads paper on arXiv](https://arxiv.org/abs/1608.01350)

Table of Content
----------------

- [Overview](#overview)
- [Notable Users](#notable-users)
- [Install](#install)
- [Configuration](#configuration)
- [Usage](#usage)
- [Benchmarks](#benchmarks)
- [Examples](#examples)

Overview
--------

In this package's context, the keys are distributed among partitions and partitions are distributed among members as well. 

When you create a new consistent instance or call `Add/Remove`:

* The member's name is hashed and inserted into the hash ring,
* Average load is calculated by the algorithm defined in the paper,
* Partitions are distributed among members by hashing partition IDs and none of them exceed the average load.

Average load cannot be exceeded. So if all members are loaded at the maximum while trying to add a new member, it panics.

When you want to locate a key by calling `LocateKey`:

* The key(byte slice) is hashed,
* The result of the hash is mod by the number of partitions,
* The result of this modulo - `MOD(hash result, partition count)` - is the partition in which the key will be located,
* Owner of the partition is already determined before calling `LocateKey`. So it returns the partition owner immediately.

No memory is allocated by `consistent` except hashing when you want to locate a key.

Note that the number of partitions cannot be changed after creation. 

Notable Users
-------------

[buraksezer/consistent](https://github.com/buraksezer/consistent) is used at production by the following projects:

* [olric-data/olric](https://github.com/buraksezer/olric): Distributed, in-memory key/value store and cache.
* [aws/amazon-cloudwatch-agent-operator](https://github.com/aws/amazon-cloudwatch-agent-operator): The Amazon CloudWatch Agent Operator is software developed to manage the CloudWatch Agent on Kubernetes.
* [Azure/prometheus-collector](https://github.com/Azure/prometheus-collector): Azure Monitor managed service for Prometheus. 
* [open-telemetry/opentelemetry-operator](https://github.com/open-telemetry/opentelemetry-operator): Kubernetes Operator for OpenTelemetry Collector.
* [weibocom/motan-go](https://github.com/weibocom/motan-go/): A cross-language remote procedure call(RPC) framework for rapid development of high performance distributed services.
* [vllm-project/aibrix](https://github.com/vllm-project/aibrix): Cost-efficient and pluggable Infrastructure components for GenAI inference.
* [erda-project/erda](https://github.com/erda-project/erda): An enterprise-grade Cloud-Native application platform for Kubernetes.
* [giantswarm/starboard-exporter](https://github.com/giantswarm/starboard-exporter): Exposes Prometheus metrics from [Starboard](https://github.com/aquasecurity/starboard)'s `VulnerabilityReport`, `ConfigAuditReport`, and other custom resources (CRs).
* [megaease/easegress](https://github.com/megaease/easegress): A Cloud Native traffic orchestration system.
* [smartcontractkit/chainlink](https://github.com/smartcontractkit/chainlink): Oracle platform bringing the capital markets onchain and the market leader powering the majority of decentralized finance.
* [rudderlabs/keydb](https://github.com/rudderlabs/keydb): KeyDB is a distributed key store (not a key-value store) designed to be fast, scalable, and eventually consistent.
* [celo-org/celo-blockchain](https://github.com/celo-org/celo-blockchain): Global payments infrastructure built for mobile.
* [koderover/zadig](https://github.com/koderover/zadig): Zadig is a cloud native, distributed, developer-oriented continuous delivery product.
* [mason-leap-lab/infinicache](https://github.com/mason-leap-lab/infinicache): InfiniCache: A cost-effective memory cache that is built atop ephemeral serverless functions.
* [opencord/voltha-lib-go](https://github.com/opencord/voltha-lib-go): Voltha common library code.
* [kubeedge/edgemesh](https://github.com/kubeedge/edgemesh): Simplified network and services for edge applications.
* [authorizer-tech/access-controller](https://github.com/authorizer-tech/access-controller) An implementation of a distributed access-control server that is based on Google Zanzibar - "Google's Consistent, Global Authorization System.
* [Conflux-Chain/confura](https://github.com/Conflux-Chain/confura) Implementation of an Ethereum Infura equivalent public RPC service on Conflux Network.
* [mapprotocol/atlas](https://github.com/mapprotocol/atlas): Atlas chain is a truly fast, permissionless, secure and scalable public blockchain platform.

Install
-------

With a correctly configured Go environment:

```
go get github.com/buraksezer/consistent
```

You will find some useful usage samples in [examples](https://github.com/buraksezer/consistent/tree/master/_examples) folder.

Configuration
-------------

```go
type Config struct {
	// Hasher is responsible for generating unsigned, 64 bit hash of provided byte slice.
	Hasher Hasher

	// Keys are distributed among partitions. Prime numbers are good to
	// distribute keys uniformly. Select a big PartitionCount if you have
	// too many keys.
	PartitionCount int

	// Members are replicated on consistent hash ring. This number controls
	// the number each member is replicated on the ring.
	ReplicationFactor int

	// Load is used to calculate average load. See the code, the paper and Google's 
	// blog post to learn about it.
	Load float64
}
```

Any hash algorithm can be used as hasher which implements Hasher interface. Please take a look at the *Sample* section for an example.

Usage
-----

`LocateKey` function finds a member in the cluster for your key:
```go
// With a properly configured and initialized consistent instance
key := []byte("my-key")
member := c.LocateKey(key)
```
It returns a thread-safe copy of the member you added before.

The second most frequently used function is `GetClosestN`. 

```go
// With a properly configured and initialized consistent instance

key := []byte("my-key")
members, err := c.GetClosestN(key, 2)
```

This may be useful to find backup nodes to store your key.

Benchmarks
----------
On an Apple M4 Pro:

```
$ go test -run=XXX -bench=. -benchmem

goos: darwin
goarch: arm64
pkg: github.com/buraksezer/consistent
cpu: Apple M4 Pro
BenchmarkNew/partitions=23/members=8-12         	   37102	     30153 ns/op	   24093 B/op	     564 allocs/op
BenchmarkNew/partitions=23/members=64-12        	     784	   1515623 ns/op	  176063 B/op	    4117 allocs/op
BenchmarkNew/partitions=271/members=8-12        	   23420	     51233 ns/op	   46418 B/op	     821 allocs/op
BenchmarkNew/partitions=271/members=64-12       	     775	   1538918 ns/op	  203440 B/op	    4387 allocs/op
BenchmarkAddRemove/partitions=23/members=8-12   	   94064	     12804 ns/op	    6009 B/op	     196 allocs/op
BenchmarkAddRemove/partitions=23/members=64-12  	   21493	     55918 ns/op	    8334 B/op	     202 allocs/op
BenchmarkAddRemove/partitions=271/members=8-12  	   21342	     56083 ns/op	   50246 B/op	     708 allocs/op
BenchmarkAddRemove/partitions=271/members=64-12 	   10000	    106469 ns/op	   63068 B/op	     741 allocs/op
BenchmarkFindPartitionID-12                     	343323572	         3.315 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocateKey/members=8-12                 	100000000	        10.85 ns/op	       0 B/op	       0 allocs/op
BenchmarkLocateKey/members=512-12               	100000000	        10.42 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetPartitionOwner-12                   	197541891	         6.070 ns/op	       0 B/op	       0 allocs/op
BenchmarkAverageLoad-12                         	321314962	         3.728 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetClosestN/members=8-12               	 2870060	       416.5 ns/op	     416 B/op	      17 allocs/op
BenchmarkGetClosestN/members=64-12              	  249660	      4763 ns/op	    6664 B/op	      85 allocs/op
BenchmarkGetClosestN/members=512-12             	   23527	     51169 ns/op	   53864 B/op	     542 allocs/op
BenchmarkGetMembers/members=8-12                	21827589	        54.65 ns/op	     128 B/op	       1 allocs/op
BenchmarkGetMembers/members=64-12               	 2895001	       393.3 ns/op	    1152 B/op	       1 allocs/op
BenchmarkGetMembers/members=512-12              	  364288	      3425 ns/op	    9472 B/op	       1 allocs/op
BenchmarkLoadDistribution/members=8-12          	 7904462	       152.5 ns/op	     256 B/op	       2 allocs/op
BenchmarkLoadDistribution/members=64-12         	  433052	      2735 ns/op	    6952 B/op	      11 allocs/op
BenchmarkLoadDistribution/members=512-12        	   51364	     23206 ns/op	   54408 B/op	      17 allocs/op
BenchmarkLocateKeyParallel-12                   	 8768479	       144.9 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/buraksezer/consistent	34.865s
```

Examples
--------

The most basic use of the consistent package should be like this. For a detailed list of functions, [visit pkg.go.dev.](https://pkg.go.dev/github.com/buraksezer/consistent)
More sample code can be found under [_examples](https://github.com/buraksezer/consistent/tree/master/_examples).

```go
package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

// In your code, you probably have a custom data type 
// for your cluster members. Just add a String function to implement 
// consistent.Member interface.
type myMember string

func (m myMember) String() string {
	return string(m)
}

// consistent package doesn't provide a default hashing function. 
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func main() {
	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	node1 := myMember("node1.olric.com")
	c.Add(node1)

	node2 := myMember("node2.olric.com")
	c.Add(node2)

	key := []byte("my-key")
	// calculates partition id for the given key
	// partID := hash(key) % partitionCount
	// the partitions are already distributed among members by Add function.
	owner := c.LocateKey(key)
	fmt.Println(owner.String())
	// Prints node2.olric.com
}
```

Another useful example is `_examples/relocation_percentage.go`. It creates a `consistent` object with 8 members and distributes partitions among them. Then adds 9th member, 
here is the result with a proper configuration and hash function:

```
bloom:consistent burak$ go run _examples/relocation_percentage.go
partID: 218 moved to node2.olric from node0.olric
partID: 173 moved to node9.olric from node3.olric
partID: 225 moved to node7.olric from node0.olric
partID:  85 moved to node9.olric from node7.olric
partID: 220 moved to node5.olric from node0.olric
partID:  33 moved to node9.olric from node5.olric
partID: 254 moved to node9.olric from node4.olric
partID:  71 moved to node9.olric from node3.olric
partID: 236 moved to node9.olric from node2.olric
partID: 118 moved to node9.olric from node3.olric
partID: 233 moved to node3.olric from node0.olric
partID:  50 moved to node9.olric from node4.olric
partID: 252 moved to node9.olric from node2.olric
partID: 121 moved to node9.olric from node2.olric
partID: 259 moved to node9.olric from node4.olric
partID:  92 moved to node9.olric from node7.olric
partID: 152 moved to node9.olric from node3.olric
partID: 105 moved to node9.olric from node2.olric

6% of the partitions are relocated
```

Moved partition count is highly dependent on your configuration and quailty of hash function. You should modify the configuration to find an optimum set of configurations
for your system.

`_examples/load_distribution.go` is also useful to understand load distribution. It creates a `consistent` object with 8 members and locates 1M key. It also calculates average 
load which cannot be exceeded by any member. Here is the result:

```
Maximum key count for a member should be around this:  147602
member: node2.olric, key count: 100362
member: node5.olric, key count: 99448
member: node0.olric, key count: 147735
member: node3.olric, key count: 103455
member: node6.olric, key count: 147069
member: node1.olric, key count: 121566
member: node4.olric, key count: 147932
member: node7.olric, key count: 132433
```

Average load can be calculated by using the following formula:

```
load := (consistent.AverageLoad() * float64(keyCount)) / float64(config.PartitionCount)
```

Contributions
-------------
Please don't hesitate to fork the project and send a pull request or just e-mail me to ask questions and share ideas.

License
-------
MIT License, – see LICENSE for more details.
