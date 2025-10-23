// Copyright 2020 Intel Corporation. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package realtime

import (
	"fmt"
	"os"
	"path"
	"testing"

	cfgapi "github.com/containers/nri-plugins/pkg/apis/config/v1alpha1/resmgr/policy/realtime"
	policyapi "github.com/containers/nri-plugins/pkg/resmgr/policy"

	system "github.com/containers/nri-plugins/pkg/sysfs"
	"github.com/containers/nri-plugins/pkg/utils"
)

func findNodeWithName(name string, nodes []Node) Node {
	for _, node := range nodes {
		if node.Name() == name {
			return node
		}
	}
	panic("No node found with name " + name)
}

func TestPoolCreation(t *testing.T) {

	// Test pool creation with "real" sysfs data.

	// Create a temporary directory for the test data.
	dir, err := os.MkdirTemp("", "nri-resource-policy-test-sysfs-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// Uncompress the test data to the directory.
	err = utils.UncompressTbz2(path.Join("testdata", "sysfs.tar.bz2"), dir)
	if err != nil {
		panic(err)
	}

	tcases := []struct {
		path                    string
		name                    string
		req                     Request
		affinities              map[int]int32
		expectedRemainingNodes  []int
		expectedFirstNodeMemory memoryType
		expectedLeafNodeCPUs    int
		expectedRootNodeCPUs    int
		// TODO: expectedRootNodeMemory   int
	}{
		{
			path: path.Join(dir, "sysfs", "desktop", "sys"),
			name: "sysfs pool creation from a desktop system",
			req: &request{
				memReq:    10000,
				memLim:    10000,
				memType:   memoryAll,
				container: &mockContainer{},
			},
			expectedRemainingNodes:  []int{0},
			expectedFirstNodeMemory: memoryDRAM,
			expectedLeafNodeCPUs:    20,
			expectedRootNodeCPUs:    20,
		},
		{
			path: path.Join(dir, "sysfs", "server", "sys"),
			name: "sysfs pool creation from a server system",
			req: &request{
				memReq:    10000,
				memLim:    10000,
				memType:   memoryDRAM,
				container: &mockContainer{},
			},
			expectedRemainingNodes:  []int{0, 1, 2, 3, 4, 5, 6},
			expectedFirstNodeMemory: memoryDRAM | memoryPMEM,
			expectedLeafNodeCPUs:    28,
			expectedRootNodeCPUs:    112,
		},
		{
			path: path.Join(dir, "sysfs", "server", "sys"),
			name: "pmem request on a server system",
			req: &request{
				memReq:    10000,
				memLim:    10000,
				memType:   memoryDRAM | memoryPMEM,
				container: &mockContainer{},
			},
			expectedRemainingNodes:  []int{0, 1, 2, 3, 4, 5, 6},
			expectedFirstNodeMemory: memoryDRAM | memoryPMEM,
			expectedLeafNodeCPUs:    28,
			expectedRootNodeCPUs:    112,
		},
		{
			path: path.Join(dir, "sysfs", "4-socket-server-nosnc", "sys"),
			name: "sysfs pool creation from a 4 socket server with SNC disabled",
			req: &request{
				memReq:    10000,
				memLim:    10000,
				memType:   memoryAll,
				container: &mockContainer{},
			},
			expectedRemainingNodes:  []int{0, 1, 2, 3, 4},
			expectedFirstNodeMemory: memoryDRAM,
			expectedLeafNodeCPUs:    36,
			expectedRootNodeCPUs:    36 * 4,
		},
	}
	for _, tc := range tcases[:1] {
		t.Run(tc.name, func(t *testing.T) {
			sys, err := system.DiscoverSystemAt(tc.path)
			if err != nil {
				panic(err)
			}

			policyOptions := &policyapi.BackendOptions{
				Cache:  &mockCache{},
				System: sys,
				Config: &cfgapi.Config{
					ReservedResources: cfgapi.Constraints{
						cfgapi.CPU: "750m",
					},
				},
			}

			log.EnableDebug(true)
			policy := New().(*policy)
			if err := policy.Setup(policyOptions); err != nil {
				log.Warnf("failed to setup test policy: %v", err)
			}
			log.EnableDebug(false)

			log.EnableDebug(true)

			log.Infof("reserved CPUs: %s", policy.root.GetSupply().ReservedCPUs().String())
			log.Infof("isolated CPUs: %s", policy.root.GetSupply().IsolatedCPUs().String())
			log.Infof("sharable CPUs: %d", policy.root.GetSupply().SharableCPUs().String())

			if policy.root.GetSupply().SharableCPUs().Size()+policy.root.GetSupply().IsolatedCPUs().Size()+policy.root.GetSupply().ReservedCPUs().Size() != tc.expectedRootNodeCPUs {
				t.Errorf("Expected %d CPUs, got %d", tc.expectedRootNodeCPUs,
					policy.root.GetSupply().SharableCPUs().Size()+policy.root.GetSupply().IsolatedCPUs().Size()+policy.root.GetSupply().ReservedCPUs().Size())
			} else {
				fmt.Printf("Root node has %d CPUs", tc.expectedRootNodeCPUs)
			}

			for _, p := range policy.pools {
				if p.IsLeafNode() {
					if len(p.Children()) != 0 {
						t.Errorf("Leaf node %v had %d children", p, len(p.Children()))
					} else {
						fmt.Printf("Leaf node %v had no children", p)
					}
					if p.GetSupply().SharableCPUs().Size()+p.GetSupply().IsolatedCPUs().Size()+p.GetSupply().ReservedCPUs().Size() != tc.expectedLeafNodeCPUs {
						t.Errorf("Expected %d CPUs, got %d (%s)", tc.expectedLeafNodeCPUs,
							p.GetSupply().SharableCPUs().Size()+p.GetSupply().IsolatedCPUs().Size()+p.GetSupply().ReservedCPUs().Size(),
							p.GetSupply().DumpCapacity())
					} else {
						fmt.Printf("Leaf node %v had %d CPUs", p, tc.expectedLeafNodeCPUs)
					}
				}
			}

			scores, filteredPools := policy.sortPoolsByScore(tc.req, tc.affinities)
			fmt.Printf("scores: %v, remaining pools: %v\n", scores, filteredPools)

			if len(filteredPools) != len(tc.expectedRemainingNodes) {
				t.Errorf("Wrong number of nodes in the filtered pool: expected %d but got %d", len(tc.expectedRemainingNodes), len(filteredPools))
			} else {
				fmt.Printf("Got expected number of nodes in the filtered pool: %d", len(filteredPools))
			}

			for _, id := range tc.expectedRemainingNodes {
				found := false
				for _, node := range filteredPools {
					if node.NodeID() == id {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Did not find id %d in filtered pools: %s", id, filteredPools)
				} else {
					fmt.Printf("Found expected id %d in filtered pools", id)
				}
			}

			if len(filteredPools) > 0 && filteredPools[0].GetMemoryType() != tc.expectedFirstNodeMemory {
				t.Errorf("Expected first node memory type %v, got %v", tc.expectedFirstNodeMemory, filteredPools[0].GetMemoryType())
			} else {
				fmt.Printf("First node had expected memory type %v", tc.expectedFirstNodeMemory)
			}
		})
	}
}
