# 第10章 kubelet 中的 cgroupManager 解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 10 章 — kubelet 中的 cgroupManager 解读
> **源码入口**: `pkg/kubelet/cm/cgroup_manager_linux.go`、`pkg/kubelet/cm/pod_container_manager_linux.go`

---

## 核心机制一览

1. **cgroup 是资源隔离的内核机制**：kubelet 的所有资源限制（CPU share/quota、内存上限）最终都落地为 cgroup 文件系统的写操作。理解 cgroupManager 就是理解 kubelet 如何把 Pod 的 `resources.requests/limits` 转换成内核 cgroup 配置。

2. **CgroupManager 接口抽象了两种 driver**：底层实现有 `cgroupfs`（直接操作 `/sys/fs/cgroup/` 文件）和 `systemd`（通过 D-Bus 操作 systemd unit）两种。`NewCgroupManager` 根据 `cgroupDriver` 配置决定使用哪种 adapter，上层代码完全屏蔽差异。

3. **节点的 cgroup 目录是三层结构**：kubelet 启动时在 `setupNode` 中创建顶级 QoS cgroup 目录，形成 `kubepods.slice → kubepods-besteffort.slice / kubepods-burstable.slice → pod<UID>.slice` 三层层级。Pod 的 QoS 类型决定它进入哪一层。

4. **Pod cgroup 的创建入口是 `EnsureExists`**：在 `syncPod` 中调用，仅当 Pod 不存在时创建。路径构造规则：`parentContainer`（由 QoS 决定）+ podUID，再拼容器 ID，最终形成完整 cgroup 路径。

5. **Pod cgroup 的删除有两阶段**：先通过 `Pids()` 收集 cgroup 下所有进程 PID，遍历 subsystem 读取 `cgroup.procs` 文件，用 `walk` 递归；再 retry kill 最多 5 次；最后调用 `cgroupManager.Destroy` 删除目录（底层 systemd driver 调用 `RemovePaths` 重试删除）。

6. **cgroup v1 与 v2 的根本差异**：v1 允许不同 controller 挂载到不同 hierarchy（实际没用，因为 controller 只能属于一个 hierarchy）；v2 强制 unified hierarchy，所有 controller 挂在同一棵树下，通过 `cgroup.controllers` 和 `cgroup.subtree_control` 文件控制启用范围，并引入 "no internal processes" 规则（进程只能在叶子节点）。

---

## 全章调用链总图

```
kubelet.New()
  │
  └─── NewContainerManager(...)                 container_manager_linux.go
         ├─── GetCgroupSubsystems()              helpers_linux.go:250
         │      └─── 读 /proc/self/mountinfo 解析已挂载的 cgroup subsystem
         └─── NewCgroupManager(subsystems, driver)  cgroup_manager_linux.go:191
                └─── 根据 driver 选择 adapter
                       ├─── cgroupfs → cgroupfs.NewManager()
                       └─── systemd  → systemd.NewLegacyManager()

kubelet.Run()
  └─── containerManager.Start()
         └─── setupNode(activePods)              container_manager_linux.go:465
                ├─── [if CgroupsPerQOS] createNodeAllocatableCgroups()  node_container_manager_linux.go:40
                │      └─── cgroupManager.Create(cgroupConfig)           cgroup_manager_linux.go:609
                └─── qosContainerManager.Start()

syncPod()
  └─── containerManager.UpdateQOSCgroups()
  └─── podContainerManager.EnsureExists(pod)    pod_container_manager_linux.go:77
         ├─── GetPodContainerName(pod)           pod_container_manager_linux.go:105
         │      ├─── GetPodQOS(pod) → parentContainer (Guaranteed/Burstable/BestEffort)
         │      └─── NewCgroupName(parent, podUID) → podContainerName
         ├─── [不存在] cgroupManager.Create(containerConfig)
         └─── [v1.22+] enforceMemoryQoS

syncLoopIteration (housekeepingCh)
  └─── HandlePodCleanups()                      kubelet_pods.go:1067
         └─── cleanupOrphanedPodCgroups()
                └─── podContainerManager.Destroy(podCgroup)  pod_container_manager_linux.go:185
                       ├─── tryKillingCgroupProcesses()
                       │      ├─── cgroupManager.Pids(podCgroup)    — 读所有 subsystem/cgroup.procs
                       │      └─── retry 5次 killOnePid()
                       └─── cgroupManager.Destroy(containerConfig)  — 删除 cgroup 目录
```

---

## §01 cgroupv1 原理介绍和 Go 代码体验 CPU/Memory 限制

| 读码目标 | 源文件（可点击） | 入口 |
|---------|----------------|------|
| cgroup 子系统说明 | 内核虚拟文件系统 `/sys/fs/cgroup/` | — |

### cgroup 文件系统

Linux 通过文件的方式将 cgroups 的功能和配置暴露给用户（VFS）。挂载示例：

```bash
mount -t cgroup -o cpu,cpuset,memory cpu_mem /cgroups/cpu_mem
```

### cpu 子系统关键参数

- `cpu.shares`：cgroup 对时间的分配权重，比如 A=1、B=2，则 B 获得 CPU 是 A 的 2 倍
- `cpu.cfs_period_us`：完全公平调度器的调整时间配额周期（默认 100000 = 100ms）
- `cpu.cfs_quota_us`：周期内可以占用的时间（-0.1 代表使用前一时期配额的 100ms 的一）
- `cpu.stat`：统计值（nr_periods、nr_throttled、nr_throttle_us）

### cpuacct 子系统

用于统计 cgroup 的 CPU 使用，`cpuacct.usage` 记录所有 CPU 使用总时间（纳秒），利用率计算：

```
(cpuacct.usage_11 - cpuacct.usage_10) / (t1 - t0) * 100
```

### cpuset 子系统

- `cpuset.cpus`：可以使用的 CPU 节点
- `cpuset.mems`：可以使用的 Memory 节点
- `cpuset.memory_migrate`：内存页的迁移是否跟着迁移？
- `cpuset.cpu_exclusive`：此 cgroup 组是否独占 CPU 核心？
- `cpuset.mem_exclusive`：此 cgroup 组是否独占 Memory 节点？
- `cpuset.mem_hardwall`：限制内核内存分配的隔离（mem 用户空间的隔离）
- `cpuset.memory_pressure_enabled`：是否需要计算 memory_pressure？

### memory 子系统关键参数

- `memory.usage_in_bytes`：当前内存使用量
- `memory.limit_in_bytes`：设置最大可用内存
- `memory.failcnt`：内存使用量超过限制的次数
- `memory.memsw.limit_in_bytes`：限制内存+swap 使用总量
- `memory.oom_control`：设置是否启动 OOM Killer
- `memory.swappiness`：设置换出内存的权重

### blkio 子系统

有两种限制方式：权重（weight，100-1000 之间）和绝对值（iops/bps）。

- `blkio.weight`：设置 blkio 权重（100-1000），子 cgroup 可继承但不能超过父值
- `blkio.throttle.read_bps_device`：每秒读字节数限制
- `blkio.throttle.write_bps_device`：每秒写字节数限制
- `blkio.throttle.read_iops_device`：每秒读操作数限制
- `blkio.throttle.write_iops_device`：每秒写操作数限制

### Go 代码体验 CPU 限制

通过 `cgexec` 命令将进程加入 cgroup 并运行：

```bash
# 准备 cpu 子系统限制进程的 cpu
mkdir /sys/fs/cgroup/cpu/my_cpu
echo 100000 > /sys/fs/cgroup/cpu/my_cpu/cpu.cfs_period_us
echo 100    > /sys/fs/cgroup/cpu/my_cpu/cpu.cfs_quota_us

# 运行 cpu 密集型程序：不加限制约 2s，加入 cgroup 后约 21s（被节流）
time cgexec -g cpu:my_cpu ./cpu_use
# 进程的 tasks 文件记录了被限制的线程 id
```

### Go 代码体验内存限制

```bash
mkdir -pv /sys/fs/cgroup/memory/my_mem
echo 314572800 > /sys/fs/cgroup/memory/my_mem/memory.limit_in_bytes  # 300MB
echo 0         > /sys/fs/cgroup/memory/my_mem/memory.swappiness

cgexec -g memory:my_mem ./mem_use
# 申请 5*100MB，前两次成功，第三次 Killed（超过 300MB 限制）
# /var/log/messages 可见 OOM killer 触发日志
```

---

## §02 cgroupv2 原理介绍

| 读码目标 | 资源 | 说明 |
|---------|------|------|
| cgroup v2 内核文档 | kernel.org/doc/html/latest/admin-guide/cgroup-v2.html | unified hierarchy |

### cgroup v1 的问题

- v1 允许多个 hierarchy，但因为 controller（控制器）只能属于一个 hierarchy，多 hierarchy 实际上没有用处

### cgroup v2 的五点改进

1. **unified hierarchy**：所有 controller 挂载到同一个 hierarchy，不存在 v1 中不同 controller 挂载到不同 hierarchy 的情况
2. **Process 只能绑定到叶子节点**：只能绑定到 cgroup 根（"/"）目录和 cgroup 目录树中的叶子节点
3. **通过 `cgroup.controllers` 和 `cgroup.subtree_control` 指定哪些 controller 可被使用**
4. **v1 版本的 task 文件和 cpuset controller 中的 `cgroup.clone_children` 文件被移除**
5. **当 cgroup 为空时的通知机制改进**：通过 `cgroup.events` 文件通知

### unified hierarchy

所有 controller 挂载到一个 hierarchies：

```bash
mount -t cgroup2 none $MOUNT_POINT
```

cgroup v2 支持的 controllers：io（Linux 4.5）、memory（Linux 4.5）、pids（Linux 4.5）、perf_event（Linux 4.11）、rdma（Linux 4.11）、cpu（Linux 4.15）。

### subtree control

每个 cgroup 下有两个关键文件：

- `cgroup.controllers`：只读，包含该 cgroup 下所有可用的 controllers
- `cgroup.subtree_control`：包含该 cgroup 下已经被开启的 controllers（是 `cgroup.controllers` 的子集）

启用/禁用 controller：`echo '+pids -memory' > x/y/cgroup.subtree_control`

### cgroup v2 结构（unified hierarchy 示意）

```
Root cgroup: /sys/fs/cgroup
  cgroup.controllers: cpu io memory pids
  cgroup.subtree_control: cpu io memory pids
    │
    ├─── system.slice
    │      cgroup.controllers: cpu io memory pids
    │      cgroup.subtree_control: cpu io memory
    │        └─── crond.service
    │
    └─── workload.slice
           cgroup.controllers: cpu io memory pids
           cgroup.subtree_control: cpu io memory
             └─── workload-support.slice
                    cgroup.subtree_control: cpu memory
                      └─── smc_proxy.service (cpu memory)
```

### "no internal processes" 规则

与 v1 不同，cgroup v2 只能将进程绑定到叶子节点，不能绑定进程到任何一个已开启 controller 的 subgroup 中（`echo 0 > cgroup.proc` 会失败）。

### cgroup.events 文件

- v1 使用 `release_agent` 和 `notify_on_release`，v2 中被移除
- 替代方案：`cgroup.events` 文件（只读 key-value），`populated` key 表示 cgroup 是否包含进程（0=没有，1=有）

### 后代 cgroup 数量限制

- `cgroup.max.depth`（Linux 4.14）：定义子 cgroup 最大深度，0 表示不能创建 cgroup，默认 max
- `cgroup.max.descendants`（Linux 4.14）：可创建的活跃 cgroup 目录最大数量，默认 max

---

## §03 kubelet 中的 cgroupManager 解析和节点 QoS 顶级目录创建

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| CgroupManager 接口 | [types.go](kubernetes/pkg/kubelet/cm/types.go) | `CgroupManager interface:70` |
| cgroupManagerImpl 结构体 | [cgroup_manager_linux.go](kubernetes/pkg/kubelet/cm/cgroup_manager_linux.go) | `cgroupManagerImpl:179` |
| NewCgroupManager | [cgroup_manager_linux.go](kubernetes/pkg/kubelet/cm/cgroup_manager_linux.go) | `NewCgroupManager:191` |
| GetCgroupSubsystems | [helpers_linux.go](kubernetes/pkg/kubelet/cm/helpers_linux.go) | `GetCgroupSubsystems:250` |
| setupNode | [container_manager_linux.go](kubernetes/pkg/kubelet/cm/container_manager_linux.go) | `setupNode:465` |
| createNodeAllocatableCgroups | [node_container_manager_linux.go](kubernetes/pkg/kubelet/cm/node_container_manager_linux.go) | `createNodeAllocatableCgroups:40` |
| cgroupManager.Create | [cgroup_manager_linux.go](kubernetes/pkg/kubelet/cm/cgroup_manager_linux.go) | `Create:609` |

### CgroupManager 接口

```go
// pkg/kubelet/cm/types.go:70
type CgroupManager interface {
    // 创建并应用 cgroup 配置
    Create(*CgroupConfig) error
    // 销毁 cgroup
    Destroy(*CgroupConfig) error
    // 更新 cgroup 配置（不做副作用检查）
    Update(*CgroupConfig) error
    // 检查 cgroup 是否存在
    Exists(name CgroupName) bool
    // 获取 cgroup 名对应的文件系统路径
    Name(name CgroupName) string
    // 获取 cgroup 下所有进程 PID
    Pids(name CgroupName) []int
    // 减少 CPU 限额（驱逐时调用）
    ReduceCPULimits(cgroupName CgroupName) error
}
```

### cgroupManagerImpl 结构体

```go
// pkg/kubelet/cm/cgroup_manager_linux.go:179
type cgroupManagerImpl struct {
    subsystems *CgroupSubsystems  // 已挂载的 cgroup 子系统信息
    adapter    libcontainerCgroupManagerType  // cgroupfs 或 systemd 的适配器
}
```

**CgroupSubsystems** 是对已挂载 cgroup subsystem 的缓存：

```go
type CgroupSubsystems struct {
    // e.g.: "/sys/fs/cgroup/cpu" -> ["cpu", "cpuacct"]
    Mounts []libcontainercgroups.Mount
    // e.g.: "cpu" -> "/sys/fs/cgroup/cpu"
    MountPoints map[string]string
}
```

### CgroupManager 的初始化

kubelet 在 `NewContainerManager` 中初始化（`container_manager_linux.go:215`）：

```go
cgroupManager := NewCgroupManager(subsystems, nodeConfig.CgroupDriver)
```

**入参分析**：`nodeConfig.CgroupDriver` 决定底层 adapter：

- **cgroupfs**：对应 `cgroupfs.NewManager()`，直接操作 `/sys/fs/cgroup/` 文件
- **systemd**：对应 `systemd.NewLegacyManager()`，通过 D-Bus 调用 systemd API

### GetCgroupSubsystems 获取挂载信息

`GetCgroupSubsystems` 通过解析 `/proc/self/mountinfo` 获取节点上已挂载的 cgroup 子系统：

```go
// pkg/kubelet/cm/helpers_linux.go:250
func GetCgroupSubsystems() (*CgroupSubsystems, error) {
    // get all cgroup mounts
    allCgroups, err := libcontainercgroups.GetCgroupMounts(true)
    // ...
    // 找最短路径（挂载点越短越靠近 /）
    mountPoints := make(map[string]string)
    for _, mount := range allCgroups {
        for _, subsystem := range mount.Subsystems {
            previous := mountPoints[subsystem]
            if previous == "" || len(previous) > len(mount.Mountpoint) {
                mountPoints[subsystem] = mount.Mountpoint
            }
        }
    }
    return &CgroupSubsystems{...}
}
```

### NewCgroupManager 分析

```go
// pkg/kubelet/cm/cgroup_manager_linux.go:191
func NewCgroupManager(cs *CgroupSubsystems, cgroupDriver string) CgroupManager {
    managerType := libcontainerCgroupfs
    if cgroupDriver == string(libcontainerSystemd) {
        managerType = libcontainerSystemd
    }
    return &cgroupManagerImpl{
        subsystems: cs,
        adapter:    newLibcontainerAdapter(managerType),
    }
}
```

底层的两个实现：

- **cgroupfs** driver → `cgroupfs.NewManager()`，源码位于 `vendor/github.com/opencontainers/runc/libcontainer/cgroups/fs/fs.go`
- **systemd** driver → `systemd.NewLegacyManager()`，源码位于 `vendor/github.com/opencontainers/runc/libcontainer/cgroups/systemd/v1.go`

### cgroupManager 的应用：节点 QoS 顶级目录创建

`setupNode` 在 kubelet 启动时调用（`container_manager_linux.go:465`），根据 `--cgroups-per-qos` 参数（默认 true）创建 node cgroups：

```go
func (cm *containerManagerImpl) setupNode(activePods ActivePodsFunc) error {
    if cm.NodeConfig.CgroupsPerQOS {
        if err := cm.createNodeAllocatableCgroups(); err != nil {
            return err
        }
        err = cm.qosContainerManager.Start(cm.GetNodeAllocatableAbsolute, activePods)
    }
}
```

**createNodeAllocatableCgroups** 计算可分配资源（节点总量减去 `SystemReserved` 和 `KubeReserved`），然后用剩余量去 cgroupManager 查找或创建顶级 cgroup：

```go
// pkg/kubelet/cm/node_container_manager_linux.go:40
func (cm *containerManagerImpl) createNodeAllocatableCgroups() error {
    cgroupConfig := &CgroupConfig{
        Name:               cm.cgroupRoot,
        ResourceParameters: getCgroupConfig(nodeAllocatable),
    }
    if cm.cgroupManager.Exists(cgroupConfig.Name) {
        return nil
    }
    cm.cgroupManager.Create(cgroupConfig)
}
```

### cgroupManager.Create 解析

`Create` 在底层调用 adapter 的 `newManager` 创建 libcontainer cgroup 对象，再调用 `manager.Apply(-1)` 创建 subsystem 顶级目录：

```go
// pkg/kubelet/cm/cgroup_manager_linux.go:609
func (m *cgroupManagerImpl) Create(cgroupConfig *CgroupConfig) error {
    // ...
    manager, err := m.adapter.newManager(libcontainerCgroupConfig, nil)
    manager.Apply(-1)  // -1 代表创建 subsystem 顶级目录（不绑定进程）
}
```

对于 **systemd driver**，`Apply` 最终调用 `legacyManager.Apply(pid)`，通过 D-Bus 在 `system.slice` 下创建对应的 slice unit：

```go
// vendor/.../cgroups/systemd/v1.go
func (m *legacyManager) Apply(pid int) error {
    // ...
    // 通过 systemd D-Bus 创建 slice
    properties = append(properties, systemdDbus.PropDescription("libcontainer container "+c.Name))
    // ...
    err = m.joinCgroupsWithSymlink(unitName, properties)
}
```

**最终创建的 QoS 顶级目录**：

```
/sys/fs/cgroup/systemd/kubepods.slice
  ├── kubepods-besteffort.slice   ← BestEffort Pod
  ├── kubepods-burstable.slice    ← Burstable Pod
  └── kubepods-pod<UID>.slice     ← Guaranteed Pod（直接挂在 kubepods 下）
```

---

## §04 containerManager 应用之创建/删除容器 cgroup 目录

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| EnsureExists | [pod_container_manager_linux.go](kubernetes/pkg/kubelet/cm/pod_container_manager_linux.go) | `EnsureExists:77` |
| GetPodContainerName | [pod_container_manager_linux.go](kubernetes/pkg/kubelet/cm/pod_container_manager_linux.go) | `GetPodContainerName:105` |
| HandlePodCleanups | [kubelet_pods.go](kubernetes/pkg/kubelet/kubelet_pods.go) | `HandlePodCleanups:1067` |
| Destroy | [pod_container_manager_linux.go](kubernetes/pkg/kubelet/cm/pod_container_manager_linux.go) | `Destroy:185` |

### 创建 Pod cgroup：podContainerManager.EnsureExists

**EnsureExists 是 kubelet syncPod 时调用的**，位置 `kubelet.go`，注释说明两种情况不会创建/更新 cgroup：Pod 被 kill 了，或者 Pod 只运行一次（RestartPolicy=Never）。

#### 第一步：GetPodContainerName 构造 cgroup 路径

```go
// pkg/kubelet/cm/pod_container_manager_linux.go:105
func (m *podContainerManagerImpl) GetPodContainerName(pod *v1.Pod) (CgroupName, string) {
    podQOS := v1qos.GetPodQOS(pod)
    // 根据 QoS 确定父 cgroup 目录
    var parentContainer CgroupName
    switch podQOS {
    case v1.PodQOSGuaranteed:
        parentContainer = m.qosContainersInfo.Guaranteed
    case v1.PodQOSBurstable:
        parentContainer = m.qosContainersInfo.Burstable
    case v1.PodQOSBestEffort:
        parentContainer = m.qosContainersInfo.BestEffort
    }
    // 拼 podUID 形成完整路径
    podContainer := GetPodCgroupNameSuffix(pod.UID)
    return NewCgroupName(parentContainer, podContainer), ...
}
```

Burstable 类型 pod 的 `parentContainer` 就是 `["kubepods", "burstable", "pod1234-abcd-5678-efgh"]`。

最终 `podContainerName` 形如：

```
/sys/fs/cgroup/systemd/kubepods.slice/kubepods-pod4115089e_088b_4fb1_8884_b3f163efa48c.slice/cri-...
```

#### 第二步：创建 pod cgroup

```go
// pod_container_manager_linux.go:77（EnsureExists 节选）
alreadyExists := m.Exists(pod)
if !alreadyExists {
    // 判断是否启用 MemoryQoS（v1.22 新特性，cgroup v2 内存控制器）
    enforceMemoryQoS := false
    if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.MemoryQoS) &&
        libcontainercgroups.IsCgroup2UnifiedMode() {
        enforceMemoryQoS = true
    }
    // 创建 CgroupConfig，ResourceParameters 来自 Pod 的 request 配置
    containerConfig := &CgroupConfig{
        Name:               podContainerName,
        ResourceParameters: ResourceConfigForPod(pod, m.enforceCPULimits, m.cpuCFSQuotaPeriod, enforceMemoryQoS),
    }
    m.cgroupManager.Create(containerConfig)
}
```

**ResourceConfigForPod** 把 Pod 中所有容器的 `resources.requests` 汇总为 cgroup 配置（cpu shares、memory limit 等）。

#### systemd cgroup v1 目录结构验证

创建三种 QoS 的 Pod 后，在节点查看：

```
/sys/fs/cgroup/systemd/kubepods.slice/
  ├── kubepods-pod<UID>.slice/              ← Guaranteed
  │     └── cri-<containerID>.scope/
  │           cpu.shares=1024, cpu.cfs_quota_us=100000
  ├── kubepods-besteffort.slice/            ← BestEffort
  │     └── kubepods-besteffort-pod<UID>.slice/
  └── kubepods-burstable.slice/             ← Burstable
        └── kubepods-burstable-pod<UID>.slice/
```

CPU 配置示例（cpu.cfs_period_us 默认 100000=100ms）：

```bash
cat /sys/fs/cgroup/cpu/kubepods/pod<UID>/cpu.cfs_period_us  # 100000
cat /sys/fs/cgroup/cpu/kubepods/pod<UID>/cpu.cfs_quota_us   # 100m CPU -> 10000
cat /sys/fs/cgroup/cpu/kubepods/pod<UID>/cpu.shares          # 102
```

### 删除 Pod cgroup：HandlePodCleanups → Destroy

**HandlePodCleanups** 在 syncLoop 的 `housekeepingCh` 定时触发（`kubelet_pods.go:1067`），调用 `cleanupOrphanedPodCgroups` 清理不再运行的 Pod cgroup。

#### cleanupOrphanedPodCgroups 流程

```
cleanupOrphanedPodCgroups()
  │
  ├─── 遍历 cgroupManager 管理的所有 pod cgroup
  ├─── 找到不在 running pod 列表中的 cgroup（orphan）
  │      ├─── 若 pod 仍在 running set，跳过
  │      └─── 若 memory 类型 volume 存在，不能删（等 kubelet 清理 volume 后再删）
  └─── podContainerManager.Destroy(podCgroup)
```

#### podContainerManager.Destroy 解析（pod_container_manager_linux.go:185）

**第一阶段：kill 所有进程**

```go
// 先 kill 所有 cgroup 下的进程
if err := m.tryKillingCgroupProcesses(podCgroup); err != nil {
    return fmt.Errorf("failed to kill all the processes attached to the %v cgroups: %v", ...)
}
```

**cgroupManager.Pids 方法**：遍历所有 subsystem 的挂载点，读取对应目录下的 `cgroup.procs` 文件获取 PID，再用 walk 递归子目录：

```go
func getCgroupProcs(dir string) ([]int, error) {
    procsFile := filepath.Join(dir, "cgroup.procs")
    // 扫描文件中每一行 PID
    for s.Scan() {
        pid, _ := strconv.Atoi(s.Text())
        out = append(out, pid)
    }
}
```

`cgroup.procs` 实验验证：nginx worker 进程等会出现在对应 cgroup 目录的 `cgroup.procs` 文件中。

**retry kill（最多 5 次）**：

```go
for i := 0; i < 5; i++ {
    for _, pid := range pidsToKill {
        if err := m.killOnePid(pid); err == nil {
            removed[pid] = true
        }
    }
    if len(errlist) == 0 {
        return nil  // 全部 kill 成功
    }
}
```

**第二阶段：删除 cgroup 目录**

```go
containerConfig := &CgroupConfig{Name: podGroup, ...}
m.cgroupManager.Destroy(containerConfig)
```

底层 systemd driver 调用 `RemovePaths`，带 retry 重试删除目录：

```go
// vendor/.../cgroups/systemd/utils.go
func RemovePaths(paths map[string]string) error {
    const retries = 5
    delay := 10 * time.Millisecond
    for i := 0; i <= retries; i++ {
        // ...
        for p := range paths {
            if err := os.Remove(p); err == nil {
                delete(paths, p)
            }
        }
        // 调用 runtime.GC() 帮助 GC 收集，让内核 cgroup 引用归零
        runtime.GC()
        // 如果删除失败，stat 检查是否已被删除
    }
}
```

`RemovePaths` 需要调用 `runtime.GC()` 是因为 Go 的 finalizer 可能持有 cgroup 的文件描述符，GC 能促使 finalizer 运行，让引用归零后内核才允许删除 cgroup 目录。
