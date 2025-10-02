// Copyright 2019 Intel Corporation. All Rights Reserved.
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
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	cfgapi "github.com/containers/nri-plugins/pkg/apis/config/v1alpha1/resmgr/policy/realtime"
	logger "github.com/containers/nri-plugins/pkg/log"
	"github.com/containers/nri-plugins/pkg/resmgr/cache"
	"github.com/containers/nri-plugins/pkg/resmgr/events"
	policyapi "github.com/containers/nri-plugins/pkg/resmgr/policy"
	"github.com/containers/nri-plugins/pkg/cpuallocator"
	libmem "github.com/containers/nri-plugins/pkg/resmgr/lib/memory"
	"github.com/containers/nri-plugins/pkg/utils/cpuset"
	system "github.com/containers/nri-plugins/pkg/sysfs"

)

const (
	// PolicyName is the name used to activate this policy implementation.
	PolicyName = "real-time"
	// PolicyDescription is a short description of this policy.
	PolicyDescription = "A policy for real-time workloads"

	// ColdStartDone is the event generated for the end of a container cold start period.
	ColdStartDone = "cold-start-done"
)

type allocations struct {
        policy *policy
	grants map[string]Grant
}

var opt = &cfgapi.Config{}

// policy is our runtime state for this policy.
type policy struct {
	options      *policyapi.BackendOptions // configuration common to all policies
	cfg          *cfgapi.Config // our runtime configuration
	cache        cache.Cache    // pod/container cache
        sys          system.System             // system/HW topology info
        
	allowed      cpuset.CPUSet //
	reserved     cpuset.CPUSet //
	reserveCnt   int
	rtClaimed    cpuset.CPUSet //
	isolated     cpuset.CPUSet //
        claimedCnt   int
        depth        int 
	nodeCnt      int                       // number of pools
	root         Node
	nodes        map[string]Node           //
	pools        []Node                    // pre-populated node slice for scoring, etc...

	allocations  allocations
	cpuAllocator cpuallocator.CPUAllocator //
	memAllocator *libmem.Allocator

}

// Make sure policy implements the policy.Backend interface.
var _ policyapi.Backend = &policy{}
var log logger.Logger = logger.NewLogger("policy")

// Whether we have coldstart forced off due to PMEM in movable memory zones.
var coldStartOff bool

// New creates a new uninitialized template policy instance.
func New() policyapi.Backend {
	return &policy{}
}

// Name returns the name of this policy.
func (p *policy) Name() string {
	return PolicyName
}

// Description returns the description for this policy.
func (p *policy) Description() string {
	return PolicyDescription
}

// Setup initializes the template policy instance.
func (p *policy) Setup(opts *policyapi.BackendOptions) error {
	var err error

	cfg, ok := opts.Config.(*cfgapi.Config)
	if !ok {
		return fmt.Errorf("config data of wrong type %T", opts.Config)
	}

	p.cfg = cfg
	p.cache = opts.Cache
	p.options = opts
	p.cpuAllocator = cpuallocator.NewCPUAllocator(opts.System)
	p.memAllocator, err = libmem.NewAllocator(libmem.WithSystemNodes(opts.System))
	if err != nil {
		return policyError("faile dto initialize %s policy: %w", err)
	}

	// opt = cfg
	// deafultPrio = cfg.DefaultCPUPriority.Value()
       // This part gets topology and priority info of CPUs. Guess not needed for this.

	return nil
}

// Start prepares this policy for accepting allocation/release requests.
func (p *policy) Start() error {
	log.Info("started...")

	//p.root.Dump("<post-start>")
        p.checkAllocations("  <post-start>")

        if err := p.options.PublishCPUs(p.allowed.Difference(p.reserved).List()); err != nil {
                log.Errorf("failed to publish CPU DRA resources: %v", err)
        }

	return nil
}

// Reconfigure this policy.
func (p *policy) Reconfigure(newCfg interface{}) error {
	cfg, ok := newCfg.(*cfgapi.Config)
	if !ok {
		return fmt.Errorf("config data of wrong type %T", newCfg)
	}
	p.cfg = cfg
	return nil
}

// Sync synchronizes the state of this policy.
func (p *policy) Sync(add []cache.Container, del []cache.Container) error {
	log.Info("synchronizing state...")
	
	for _, c := range del {
                if err := p.ReleaseResources(c); err != nil {
                        log.Warnf("failed to release resources for %s: %v", c.PrettyName(), err)
                }
        }

        for _, c := range add {
                if err := p.AllocateResources(c); err != nil {
                        log.Warnf("failed to allocate resources for %s: %v", c.PrettyName(), err)
                }
        }

        p.checkAllocations("  <post-sync>")

	return nil
}

func (p *policy) checkAllocations(format string, args ...interface{}) {
	var (
		prefix  = fmt.Sprintf(format, args...)
		cpuExcl = 0
		cpuPart = 0
		mem     = int64(0)
		ctr     = map[string]Grant{}
		dup     = map[string][]Grant{}
	)

	for _, g := range p.allocations.grants {
		log.Debug("%s %s (%s)", prefix, g, g.GetContainer().GetID())
		full := g.ExclusiveCPUs().Size()
		part := g.CPUPortion()
		cpuExcl += full
		cpuPart += part

		mem += g.GetMemorySize()

		_, ok := p.cache.LookupContainer(g.GetContainer().GetID())
		if !ok {
			log.Error("%s   %s STALE container among allocations, not found in cache", prefix, g)
		}

		key := g.GetContainer().PrettyName()
		old, ok := ctr[key]
		if ok {
			if len(dup[key]) == 0 {
				dup[key] = []Grant{old, g}
			} else {
				dup[key] = append(dup[key], g)
			}
		} else {
			ctr[key] = g
		}
	}

	for key, grants := range dup {
		log.Error("%s DUPLICATE allocation entries for container %s", prefix, key)
		for _, g := range grants {
			log.Error("%s   %s (%s)", prefix, g, g.GetContainer().GetID())
		}
	}

	log.Info("%s total CPU granted: %dm (%d exclusive + %dm shared), total memory granted: %s",
		prefix, 1000*cpuExcl+cpuPart, cpuExcl, cpuPart, prettyMem(mem))

}

// AllocateResources is a resource allocation request for this policy.
func (p *policy) AllocateResources(container cache.Container) error {
	log.Info("allocating resources for %s...", container.PrettyName())

	        err := p.allocateResources(container, "")
        if err != nil {
                return err
        }

        //p.root.Dump("<post-alloc>")
        p.checkAllocations("  <post-alloc %s>", container.PrettyName())

	return nil
}

func (p *policy) allocateResources(container cache.Container, poolHint string) error {
        grant, err := p.allocatePool(container, poolHint)
        if err != nil {
                return policyError("failed to allocate resources for %s: %v",
                        container.PrettyName(), err)
        }
        p.applyGrant(grant)
        p.updateSharedAllocations(&grant)

        return nil
}

// ReleaseResources is a resource release request for this policy.
func (p *policy) ReleaseResources(container cache.Container) error {
	log.Info("releasing resources of %s...", container.PrettyName())
	
	if grant, found := p.releasePool(container); found {
		p.updateSharedAllocations(&grant)
	}

        //p.root.Dump("<post-release>")
        p.checkAllocations("  <post-release %s>", container.PrettyName())

	return nil
}

// UpdateResources is a resource allocation update request for this policy.
func (p *policy) UpdateResources(container cache.Container) error {
	log.Info("(not) updating container %s...", container.PrettyName())
	
        grant, found := p.releasePool(container)
        if !found {
                log.Warnf("can't find allocation to update for %s", container.PrettyName())
                return p.AllocateResources(container)
        }
        p.updateSharedAllocations(&grant)

        poolHint := grant.GetCPUNode().Name()
        err := p.allocateResources(container, poolHint)
        if err != nil {
                return err
        }

        //p.root.Dump("<post-update>")
        p.checkAllocations("  <post-update %s>", container.PrettyName())

	return nil
}

// AllocateClaim alloctes CPUs for the claim.
func (p *policy) AllocateClaim(claim policyapi.Claim) error {
        log.Info("allocating claim %s for pods %v...", claim.String(), claim.GetPods())
        return p.allocateClaim(claim)
}

// ReleaseClaim releases CPUs of the claim.
func (p *policy) ReleaseClaim(claim policyapi.Claim) error {
        log.Info("releasing claim %s for pods %v...", claim.String(), claim.GetPods())
        return p.releaseClaim(claim)
}

// HandleEvent handles policy-specific events.
func (p *policy) HandleEvent(e *events.Policy) (bool, error) {
        log.Debug("received policy event %s.%s with data %v...", e.Source, e.Type, e.Data)

        switch e.Type {
        case events.ContainerStarted:
                c, ok := e.Data.(cache.Container)
                if !ok {
                        return false, policyError("%s event: expecting cache.Container Data, got %T",
                                e.Type, e.Data)
                }
                log.Info("triggering coldstart period (if necessary) for %s", c.PrettyName())
                return false, p.triggerColdStart(c)
        case ColdStartDone:
                id, ok := e.Data.(string)
                if !ok {
                        return false, policyError("%s event: expecting container ID Data, got %T",
                                e.Type, e.Data)
                }
                c, ok := p.cache.LookupContainer(id)
                if !ok {
                        // TODO: This is probably a race condition. Should we return nil error here?
                        return false, policyError("%s event: failed to lookup container %s", id)
                }
                log.Info("finishing coldstart period for %s", c.PrettyName())
                return p.finishColdStart(c)
        }
        return false, nil
}

// GetMetrics returns the policy-specific metrics collector.
func (p *policy) GetMetrics() policyapi.Metrics {
	return &NoMetrics{}
}

// GetTopologyZones returns the policy/pool data for 'topology zone' CRDs.
func (p *policy) GetTopologyZones() []*policyapi.TopologyZone {
	return nil
}

// ExportResourceData provides resource data to export for the container.
func (p *policy) ExportResourceData(c cache.Container) map[string]string {
	        grant, ok := p.allocations.grants[c.GetID()]
        if !ok {
                return nil
        }

        data := map[string]string{}
        shared := grant.SharedCPUs().String()
        isolated := grant.ExclusiveCPUs().Union(grant.ClaimedCPUs()).Intersection(grant.GetCPUNode().GetSupply().IsolatedCPUs())
        exclusive := grant.ExclusiveCPUs().Difference(isolated).String()
        claimed := grant.ClaimedCPUs().String()

        if grant.SharedPortion() > 0 && shared != "" {
                data[policyapi.ExportSharedCPUs] = shared
        }
        if isolated.String() != "" {
                data[policyapi.ExportIsolatedCPUs] = isolated.String()
        }
        if exclusive != "" {
                data[policyapi.ExportExclusiveCPUs] = exclusive
        }
        if claimed != "" {
                data[policyapi.ExportClaimedCPUs] = claimed
        }

        mems := grant.GetMemoryZone()
        dram := mems.And(p.memAllocator.Masks().NodesByTypes(libmem.TypeMaskDRAM))
        pmem := mems.And(p.memAllocator.Masks().NodesByTypes(libmem.TypeMaskPMEM))
        hbm := mems.And(p.memAllocator.Masks().NodesByTypes(libmem.TypeMaskHBM))
        data["ALL_MEMS"] = mems.MemsetString()
        if dram.Size() > 0 {
                data["DRAM_MEMS"] = dram.MemsetString()
        }
        if pmem.Size() > 0 {
                data["PMEM_MEMS"] = pmem.MemsetString()
        }
        if hbm.Size() > 0 {
                data["HBM_MEMS"] = hbm.MemsetString()
        }

        return data
}

type NoMetrics struct{}

func (*NoMetrics) Describe(chan<- *prometheus.Desc) {
}

func (*NoMetrics) Collect(chan<- prometheus.Metric) {
}

// clone creates a copy of the allocation.
func (a *allocations) clone() allocations {
	o := allocations{
		policy: a.policy,
		grants: make(map[string]Grant),
		//claims: make(map[string][]policyapi.Claim),
	}
	for id, grant := range a.grants {
		o.grants[id] = grant.Clone()
	}
	return o
}

// newAllocations returns a new initialized empty set of allocations.
func (p *policy) newAllocations() allocations {
	return allocations{policy: p, grants: make(map[string]Grant)}
}

// reallocateResources reallocates the given containers using the given pool hints
func (p *policy) reallocateResources(containers []cache.Container, pools map[string]string) error {
        errs := []error{}

        log.Info("reallocating resources...")

        cache.SortContainers(containers)

        for _, c := range containers {
                p.releasePool(c)
        }
        for _, c := range containers {
                log.Debug("reallocating resources for %s (%s)...", c.PrettyName(), c.GetID())

                grant, err := p.allocatePool(c, pools[c.GetID()])
                if err != nil {
                        errs = append(errs, err)
                } else {
                        p.applyGrant(grant)
                }
        }

        if err := errors.Join(errs...); err != nil {
                return err
        }

        p.updateSharedAllocations(nil)

        return nil
}

// getContainerPoolHints creates container pool hints for the current grants.
func (a *allocations) getContainerPoolHints() ([]cache.Container, map[string]string) {
        containers := make([]cache.Container, 0, len(a.grants))
        hints := make(map[string]string)
        for _, grant := range a.grants {
                c := grant.GetContainer()
                containers = append(containers, c)
                hints[c.GetID()] = grant.GetCPUNode().Name()
        }
        return containers, hints
}

