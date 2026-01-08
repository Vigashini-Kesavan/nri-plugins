// Copyright The NRI Plugins Authors. All Rights Reserved.
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

package resmgr

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "strconv"
    "strings"

    "github.com/containers/nri-plugins/pkg/kubernetes/client"
    logger "github.com/containers/nri-plugins/pkg/log"
    "github.com/containers/nri-plugins/pkg/resmgr/cache"
    system "github.com/containers/nri-plugins/pkg/sysfs"
    "google.golang.org/grpc"
    resapi "k8s.io/api/resource/v1beta2"
    "k8s.io/apimachinery/pkg/types"
    "k8s.io/dynamic-resource-allocation/kubeletplugin"
    "k8s.io/dynamic-resource-allocation/resourceslice"

        "google.golang.org/grpc/codes"
        "google.golang.org/grpc/status"
    "sort"
    "syscall"
)

type draPlugin struct {
    driverName string
    nodeName   string
    resmgr     *resmgr
    cancel     context.CancelFunc
    plugin     *kubeletplugin.Helper
    publishCh  chan<- *resourceslice.DriverResources
    claims     savedClaims
}

type UID = types.UID

var (
    dra = logger.NewLogger("dra-driver")
)

const (
    driverName = "native.cpu"
    driverKind = driverName + "/device"
    cdiVersion = "0.7.0"
    cdiEnvVar  = "DRA_CPU"
)

func (resmgr *resmgr) publishCPUs(cpuIDs []system.ID) error {
        fmt.Errorf("entering publishCPUs")
    if resmgr.dra == nil {
        return fmt.Errorf("can't publish CPUs as DRA devices, no DRA plugin")
    }

    if err := resmgr.dra.writeCDISpecFile(opt.HostRoot, cpuIDs); err != nil {
        log.Errorf("failed to write CDI Spec file: %v", err)
        return err
    }
    cpuDevices := resmgr.system.CPUsAsDRADevices(cpuIDs)
    if err := resmgr.dra.PublishResources(context.Background(), cpuDevices); err != nil {
        log.Errorf("failed to publish DRA resources: %v", err)
        return err
    }
    return nil
}

func newDRAPlugin(resmgr *resmgr) (*draPlugin, error) {
    driverPath := filepath.Join(kubeletplugin.KubeletPluginsDir, driverName)
    if err := os.MkdirAll(driverPath, 0750); err != nil {
        return nil, fmt.Errorf("failed to create driver directory %s: %w", driverPath, err)
    }

    p := &draPlugin{
        driverName: driverName,
        nodeName:   resmgr.agent.NodeName(),
        resmgr:     resmgr,
        claims:     make(map[UID]*draClaim),
    }

    p.restoreClaims()

    return p, nil
}

func (p *draPlugin) start() error {
    p.run()
    return nil
}

func (p *draPlugin) run() {
    var (
        ctx, cancel = context.WithCancel(context.Background())
        publishCh   = make(chan *resourceslice.DriverResources, 1)
    )

    go func() {
        for {
            var resources *resourceslice.DriverResources

            select {
            case <-ctx.Done():
                return
            case r, ok := <-publishCh:
                if !ok {
                    return
                }
                resources = r
            }

            if p.plugin == nil {
                if err := p.connect(); err != nil {
                    log.Errorf("failed start DRA plugin: %v", err)
                    continue
                }
            }

            if p.plugin != nil {
                if err := p.plugin.PublishResources(ctx, *resources); err != nil {
                    log.Errorf("failed to publish DRA resources: %v", err)
                } else {
                    log.Infof("published DRA resources, using %d pool(s)...", len(resources.Pools))
                }
            }

            resources = nil
        }
    }()

    p.cancel = cancel
    p.publishCh = publishCh
}

func (p *draPlugin) connect() error {
    kubeClient, err := client.New(
        client.WithKubeOrInClusterConfig(p.resmgr.agent.KubeConfig()),
        client.WithAcceptContentTypes(client.ContentTypeProtobuf, client.ContentTypeJSON),
        client.WithContentType(client.ContentTypeProtobuf),
    )
    if err != nil {
        return fmt.Errorf("can't create kube client for DRA plugin: %w", err)
    }

    options := []kubeletplugin.Option{
        kubeletplugin.DriverName(p.driverName),
        kubeletplugin.NodeName(p.nodeName),
        kubeletplugin.KubeClient(kubeClient.Clientset),
        kubeletplugin.GRPCInterceptor(p.unaryInterceptor),
    }

    log.Infof("using 02/01 DRA driverName=%s nodeName=%s", p.driverName, p.nodeName)

    plugin, err := kubeletplugin.Start(context.Background(), p, options...)
    if err != nil {
        return fmt.Errorf("failed to start DRA plugin: %w", err)
    }

    p.plugin = plugin
    return nil
}

func (p *draPlugin) stop() {
    if p == nil {
        return
    }

    if p.plugin != nil {
        p.plugin.Stop()
    }
    if p.cancel != nil {
        p.cancel()
    }

    p.plugin = nil
    p.cancel = nil
}

func (p *draPlugin) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
    dra.Info("=> gRPC call %v, handler %v\n", *info, handler)
    rpl, err := handler(ctx, req)
    dra.Info("<= gRPC reply: %+v, %v\n", rpl, err)
    return rpl, err
}

func (p *draPlugin) IsRegistered() (bool, error) {
    if p == nil || p.plugin == nil {
        return false, errors.New("DRA plugin is not initialized")
    }

    status := p.plugin.RegistrationStatus()
    if status == nil {
                dra.Info("***********dra not registered ************")
        return false, nil
    }

    var err error
    if status.Error != "" {
        err = errors.New(status.Error)
    }

    return status.PluginRegistered, err
}

func (p *draPlugin) PublishResources(ctx context.Context, devices []resapi.Device) error {
    resources := resourceslice.DriverResources{
        Pools: make(map[string]resourceslice.Pool),
    }

    maxPerPool := resapi.ResourceSliceMaxDevices
    for n := len(devices); n > 0; n = len(devices) {
        if n > maxPerPool {
            n = maxPerPool
        }
        resources.Pools["pool"+strconv.Itoa(len(resources.Pools))] = resourceslice.Pool{
            Slices: []resourceslice.Slice{
                {
                    Devices: devices[:n],
                },
            },
        }
        devices = devices[n:]
    }

    log.Infof("publishing DRA resources, using %d pool(s)...", len(resources.Pools))

    select {
    case p.publishCh <- &resources:
        return nil
    default:
    }

    return fmt.Errorf("failed to publish resources, failed to send on channel")
}

func (p *draPlugin) writeCDISpecFile(hostRoot string, cpuIDs []system.ID) error {
    spec := bytes.NewBuffer(nil)
    fmt.Fprintf(spec, "cdiVersion: %s\nkind: %s\ndevices:\n", cdiVersion, driverKind)
    for _, id := range cpuIDs {
        fmt.Fprintf(spec, "  - name: cpu%d\n", id)
        fmt.Fprintf(spec, "    containerEdits:\n")
        fmt.Fprintf(spec, "      env:\n")
        fmt.Fprintf(spec, "        - %s%d=1\n", cdiEnvVar, id)
    }

    dir := filepath.Join(hostRoot, "/var/run/cdi")
    if err := os.MkdirAll(dir, 0755); err != nil {
        return fmt.Errorf("failed to create CDI Spec directory: %w", err)
    }

    if err := os.WriteFile(filepath.Join(dir, driverName+".yaml"), spec.Bytes(), 0644); err != nil {
        return fmt.Errorf("failed to write CDI Spec file: %w", err)
    }

    return nil
}

type HKParams struct {
    APIVersion string `json:"apiVersion"`
    Kind       string `json:"kind"`
    Profile    string `json:"profile"`
}

func (p *draPlugin) PrepareResourceClaims(ctx context.Context, claims []*resapi.ResourceClaim) (map[UID]kubeletplugin.PrepareResult, error) {
    log.Infof("should prepare %d claims:", len(claims))

    undoAndErrOut := func(err error, release []*resapi.ResourceClaim) error {
        for _, c := range release {
            if relc := p.claims.del(c.UID); relc != nil {
                if undoErr := p.resmgr.policy.ReleaseClaim(relc); undoErr != nil {
                    log.Error("rollback error, failed to release claim %s: %v", c.UID, undoErr)
                }
            }
        }
        return err
    }

    defer func() {
        if err := p.saveClaims(); err != nil {
            log.Error("failed to save claims: %v", err)
        }
    }()
    p.resmgr.Lock()
    defer p.resmgr.Unlock()

    result := make(map[UID]kubeletplugin.PrepareResult)

    for i, c := range claims {
        if c == nil {
            continue
        }

        dra.Debug("  - claim #%d:", i)
        specHdr := fmt.Sprintf("    <claim #%d spec> ", i)
        statusHdr := fmt.Sprintf("    <claim #%d status> ", i)
        dra.DebugBlock(specHdr, "%s", logger.AsYaml(c.Spec))
        dra.DebugBlock(statusHdr, "%s", logger.AsYaml(c.Status))

        if old, ok := p.claims.get(c.UID); ok {
            log.Infof("claim %q already prepared, reusing it", c.UID)
            result[c.UID] = *(old.GetResult())
            continue
        }

        claim := &draClaim{ResourceClaim: c}

        if err := p.resmgr.policy.AllocateClaim(claim); err != nil {
            log.Error("failed to prepare claim %q: %v", c.UID, err)
            return nil, undoAndErrOut(err, claims[:i])
        }

        result[claim.GetUID()] = *(claim.GetResult())
        p.claims.add(claim)

        profile, err := extractHKProfile(c, "cpu", "native.cpu")
        if err != nil {
                log.Error("native.cpu config for request %q", "cpu")
            }

            switch profile {
                    case "strict", "lenient":
                if err := applyCPUProfile(profile); err != nil {
                    return nil, err
                }

            default:
                        log.Info( " unknown housekeeping mode %v", profile)
            }
        }
        //log.Info("Full claim %v", claims)

    return result, nil
}

func applyCPUProfile(profile string) error {

    if profile == "strict" {
        log.Info("strictHouseKeepingMode")
    if err := throttleConfig("/host/proc", -1); err != nil {
        return status.Errorf(codes.Internal, "throttle config failed: %v", err)
    } else {
                log.Info("throttle config update successfull")
        }
    err := setConsoleLogLevel("/host/proc", 1)
    if err != nil {
            return status.Errorf(codes.Internal,
            "failed to set console loglevel: %v", err)
    } else {
        log.Info("Console loglevel updated")
    }
    } else {
        log.Info("lenientHouseKeepingMode")

    }

    // COMMON for both strict and lenient
    if err := timerMigration("/host/proc", 0); err != nil {
        return status.Errorf(codes.Internal,
            "timer migration update failed: %v", err)
    } else {
        log.Info("timer migration update successful")
    }

    if err := affineAllIRQs("/host/proc", "16-31"); err != nil {
       log.Info("irq affinity update failed: %v", err)
    } else {
       log.Info("irq affinities updated")
    }
    return nil
}


func stringInSlice(list []string, s string) bool {
    for _, v := range list {
        if v == s {
            return true
        }
    }
    return false
}


func extractHKProfile(claim *resapi.ResourceClaim, requestName, driverName string) (string, error) {
    if claim == nil || claim.Status.Allocation == nil {
        return "", fmt.Errorf("claim %s/%s has no allocation yet", claim.Namespace, claim.Name)
    }

    for _, cfg := range claim.Status.Allocation.Devices.Config {
        if !stringInSlice(cfg.Requests, requestName) {
            continue
        }
        if cfg.Opaque == nil || cfg.Opaque.Driver != driverName {
            continue
        }

        raw := cfg.Opaque.Parameters.Raw
        if len(raw) == 0 {
            // In your dump it’s under Parameters{[bytes] <nil>} -> that’s Raw.
            return "", fmt.Errorf("claim %s/%s: empty opaque.parameters.raw", claim.Namespace, claim.Name)
        }

        var p HKParams
        if err := json.Unmarshal(raw, &p); err != nil {
            return "", fmt.Errorf("claim %s/%s: unmarshal parameters: %w", claim.Namespace, claim.Name, err)
        }
        if p.Kind != "HousekeepingMode" {
            return "", fmt.Errorf("claim %s/%s: unexpected kind %q", claim.Namespace, claim.Name, p.Kind)
        }
        if p.Profile == "" {
            return "", fmt.Errorf("claim %s/%s: missing profile", claim.Namespace, claim.Name)
        }
        return p.Profile, nil
    }

    return "", fmt.Errorf("claim %s/%s: no config found for request=%q driver=%q",
        claim.Namespace, claim.Name, requestName, driverName)
}


func affineAllIRQs(hostProc string, cpuList string) error {
    irqRoot := filepath.Join(hostProc, "irq")
    entries, err := os.ReadDir(irqRoot)
    if err != nil {
        return fmt.Errorf("readdir %s: %w", irqRoot, err)
    }

    var irqs []int
    for _, e := range entries {
        if !e.IsDir() {
            continue
        }
        n, err := strconv.Atoi(e.Name())
        if err != nil {
            continue
        }
        if n == 0 { // avoid legacy/timer IRQ 0
            continue
        }
        irqs = append(irqs, n)
    }
    sort.Ints(irqs)

    payload := strings.TrimSpace(cpuList) + "\n"

    for _, irq := range irqs {
        p := filepath.Join(irqRoot, strconv.Itoa(irq), "smp_affinity_list")

        // skip if not present
        if _, err := os.Stat(p); err != nil {
            continue
        }

        if err := os.WriteFile(p, []byte(payload), 0644); err != nil {
            // Common “can’t do that” errors from procfs: skip, don’t fail prepare.
            if errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL) {
                // log.Infof("skip irq %d: %v", irq, err)
                continue
            }
            return fmt.Errorf("write %s: %w", p, err)
        }
    }

    return nil
}

// setConsoleLogLevel sets the kernel console loglevel (equivalent to `dmesg -n <level>`)
func setConsoleLogLevel(hostProc string, level int) error {
    printkPath := filepath.Join(hostProc, "sys/kernel/printk")

    // Read current printk values
    data, err := os.ReadFile(printkPath)
    if err != nil {
        return fmt.Errorf("read printk: %w", err)
    }

    fields := strings.Fields(string(data))
    if len(fields) < 4 {
        return fmt.Errorf("unexpected printk format: %q", strings.TrimSpace(string(data)))
    }

    // Set console_loglevel (first field)
    fields[0] = fmt.Sprintf("%d", level)

    newValue := strings.Join(fields, " ") + "\n"

    if err := os.WriteFile(printkPath, []byte(newValue), 0644); err != nil {
        return fmt.Errorf("write printk: %w", err)
    }

    return nil
}

func timerMigration(hostProc string, level int) error {
    timerPath := filepath.Join(hostProc, "sys/kernel/timer_migration")

    // Read current printk values
    data, err := os.ReadFile(timerPath)
    if err != nil {
        return fmt.Errorf("read timerMigration: %w - %v", err, data)
    }

    if err := os.WriteFile(timerPath, []byte(strconv.Itoa(level)), 0644); err != nil {
        return fmt.Errorf("write timerMigration: %w", err)
    }

    return nil
}

func throttleConfig(hostProc string, level int) error {
    throttleConfigPath := filepath.Join(hostProc, "sys/kernel/sched_rt_runtime_us")

    // Read current printk values
    data, err := os.ReadFile(throttleConfigPath)
    if err != nil {
        return fmt.Errorf("read throttleConfigPath: %w - %v", err, data)
    }

    if err := os.WriteFile(throttleConfigPath, []byte(strconv.Itoa(level)), 0644); err != nil {
        return fmt.Errorf("write throttleConfigPath: %w", err)
    }

    return nil
}
func (p *draPlugin) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[UID]error, error) {

    log.Infof("should un-prepare %d claims:", len(claims))

    defer func() {
        if err := p.saveClaims(); err != nil {
            log.Error("failed to save claims: %v", err)
        }
    }()
    p.resmgr.Lock()
    defer p.resmgr.Unlock()

    result := make(map[UID]error)

    for _, c := range claims {
        log.Infof("  - un-claim %+v", c)

        if claim := p.claims.del(c.UID); claim != nil {
            if err := p.resmgr.policy.ReleaseClaim(claim); err != nil {
                log.Errorf("failed to release claim %s: %v", claim, err)
            }

                }

        result[c.UID] = nil
    }

    return result, nil
}

func (p *draPlugin) ErrorHandler(ctx context.Context, err error, msg string) {
    log.Errorf("resource slice publishing error: %v (%s)", err, msg)
}

func (p *draPlugin) saveClaims() error {
    p.resmgr.cache.SetEntry("claims", p.claims)
    return p.resmgr.cache.Save()
}

func (p *draPlugin) restoreClaims() {
    claims := make(savedClaims)
    restored, err := p.resmgr.cache.GetEntry("claims", &claims)

    if err != nil {
        if err != cache.ErrNoEntry {
            log.Error("failed to restore claims: %v", err)
        }
        p.claims = make(savedClaims)
    } else {
        if restored == nil {
            p.claims = make(savedClaims)
        } else {
            p.claims = *restored.(*savedClaims)
        }
    }
}

type draClaim struct {
    *resapi.ResourceClaim
    pods []UID
    devs []system.ID
}

func (c *draClaim) GetUID() UID {
    if c == nil || c.ResourceClaim == nil {
        return ""
    }
    return c.UID
}

func (c *draClaim) GetPods() []UID {
    if c == nil || c.ResourceClaim == nil {
        return nil
    }
    if c.pods != nil {
        return c.pods
    }

    var pods []UID
    for _, r := range c.Status.ReservedFor {
        if r.Resource == "pods" {
            pods = append(pods, r.UID)
        }
    }
    c.pods = pods

    return c.pods
}

func (c *draClaim) GetDevices() []system.ID {
    if c == nil || c.ResourceClaim == nil {
        return nil
    }

    if c.devs != nil {
        return c.devs
    }

    var ids []system.ID
    for _, r := range c.Status.Allocation.Devices.Results {
        num := strings.TrimPrefix(r.Device, "cpu")
        i, err := strconv.ParseInt(num, 10, 32)
        if err != nil {
            log.Errorf("failed to parse CPU ID %q: %v", num, err)
            continue
        }
        ids = append(ids, system.ID(i))
    }
    c.devs = ids

    return c.devs
}

func (c *draClaim) GetResult() *kubeletplugin.PrepareResult {
    result := &kubeletplugin.PrepareResult{}
    for _, alloc := range c.Status.Allocation.Devices.Results {
        result.Devices = append(result.Devices,
            kubeletplugin.Device{
                Requests:     []string{alloc.Request},
                DeviceName:   alloc.Device,
                CDIDeviceIDs: []string{driverKind + "=" + alloc.Device},
            })
    }
    return result
}

func (c *draClaim) String() string {
    return fmt.Sprintf("<CPU claim %s (CPUs %v for pods %v)>",
        c.GetUID(), c.GetDevices(), c.GetPods())
}

func (c *draClaim) MarshalJSON() ([]byte, error) {
    return json.Marshal(c.ResourceClaim)
}

func (c *draClaim) UnmarshalJSON(b []byte) error {
    c.ResourceClaim = &resapi.ResourceClaim{}
    return json.Unmarshal(b, c.ResourceClaim)
}

type savedClaims map[UID]*draClaim

func (s *savedClaims) add(c *draClaim) {
    (*s)[c.UID] = c
}

func (s *savedClaims) del(uid UID) *draClaim {
    c := (*s)[uid]
    delete(*s, uid)
    return c
}

func (s *savedClaims) get(uid UID) (*draClaim, bool) {
    c, ok := (*s)[uid]
    return c, ok
}
