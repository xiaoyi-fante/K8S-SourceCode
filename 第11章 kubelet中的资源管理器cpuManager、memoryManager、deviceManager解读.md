# 第11章 kubelet 中的资源管理器 cpuManager、memoryManager、deviceManager 解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 11 章 — kubelet 资源管理器三件套
> **源码入口**: `pkg/kubelet/cm/topologymanager/topology_manager.go`、`pkg/kubelet/cm/cpumanager/cpu_manager.go`、`pkg/kubelet/cm/memorymanager/memory_manager.go`、`pkg/kubelet/cm/devicemanager/manager.go`

---

## 核心机制一览

1. **TopologyManager 是三个管理器的协调中枢**：cpuManager、memoryManager、deviceManager 都实现了 `HintProvider` 接口，在 Pod 准入时向 TopologyManager 提供 NUMA 亲和性 Hint，由 TopologyManager 将多个 Hint 合并为最优亲和性后回传。

2. **TopologyHint 的本质是 NUMA BitMask + Preferred 标志**：`NUMANodeAffinity` 是一个位掩码（哪些 NUMA 节点可用），`Preferred` 表示这是否是设备的最优分配。策略（none/best-effort/restricted/single-numa-node）对 Preferred 的要求不同。

3. **cpuManager 静态策略只服务 Guaranteed Pod**：只有请求 CPU 为整数且 QoS=Guaranteed 的容器才会绑定独占 cpuset；其余容器放入共享池（cfs 调度）。分配算法 `takeByTopology` 优先填满整个 socket，再填满整个 core，最后才取散列的 CPU 线程，实现 NUMA 拓扑感知。

4. **cpuManager 的 reconcileState 周期同步 cpuset**：每隔 `reconcilePeriod` 遍历 activePods，对每个 Guaranteed 容器验证其 cpuset 是否与 stateMemory 一致，不一致则调用 container runtime 更新。这是 cpuset 不丢失的保障。

5. **memoryManager 静态策略只服务 Guaranteed Pod，三个限制**：①无法同时支持单个和跨 NUMA 节点分配；②只能为 Guaranteed Pod 服务；③Pod/容器启停顺序不同会导致内存碎片（alpha 阶段无整理机制）。

6. **deviceManager 的注册/分配是两条独立路径**：注册路径：device plugin 通过 Unix socket 调用 `Register` gRPC → kubelet 调用 `addEndpoint` 建立长连接 → Endpoint 持续 `ListAndWatch`；分配路径：Pod 准入时 `Allocate` 遍历容器 → `allocateContainerResources` → 调用 device plugin 的 `Allocate` gRPC 取得设备信息写入容器运行时配置。

---

## 全章调用链总图

```
kubelet.New()
  │
  ├─── NewContainerManager()
  │       │
  │       ├─── topologymanager.NewManager()       topology_manager.go:119
  │       │       └─── 创建 scope (container/pod) + policy (none/best-effort/restricted/single-numa)
  │       │
  │       ├─── cpumanager.NewManager()             cpu_manager.go:143
  │       │       └─── topologyManager.AddHintProvider(cpuManager)
  │       │
  │       ├─── memorymanager.NewManager()          memory_manager.go:123
  │       │       └─── topologyManager.AddHintProvider(memoryManager)
  │       │
  │       └─── devicemanager.NewManagerImpl()      manager.go:127
  │               └─── topologyManager.AddHintProvider(deviceManager)
  │
kubelet.Run()
  │
  ├─── containerManager.Start()
  │       ├─── cpuManager.Start()     — 启动 reconcileState 周期同步  cpu_manager.go:198
  │       ├─── memoryManager.Start()  — 读 checkpoint, 恢复状态
  │       └─── deviceManager.Start()  — 启动 kubelet.sock gRPC Server  manager.go:241
  │                                     → 等待 device plugin 调用 Register
  │
Pod 准入阶段
  │
  └─── topologyManager.Admit()                     topology_manager.go:186
          │
          └─── scope.Admit(pod)
                  │
                  ├─── accumulateProvidersHints()   — 向每个 HintProvider 收集 Hint
                  │       ├─── cpuManager.GetTopologyHints()
                  │       ├─── memoryManager.GetTopologyHints()
                  │       └─── deviceManager.GetTopologyHints()
                  │
                  ├─── calculateAffinity()          — 合并 Hints 取交集
                  │
                  └─── allocateAlignedResources()   — 调用各 provider.Allocate()
                          ├─── cpuManager.Allocate()    → staticPolicy.Allocate() cpu_manager.go:218
                          ├─── memoryManager.Allocate() → staticPolicy.Allocate() policy_static.go:87
                          └─── deviceManager.Allocate()                            manager.go:369

容器启动阶段
  │
  └─── startContainer() → internalLifecycle.PreStartContainer()  kuberuntime_container.go
          ├─── cpuManager.AddContainer()     cpu_manager.go:248
          ├─── memoryManager.AddContainer()  memory_manager.go:180
          └─── topologyManager.AddContainer()
```

---

## §01 TopologyManager 分析

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口定义 | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `Manager interface:42` |
| manager 结构体 | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `manager struct:56` |
| HintProvider 接口 | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `HintProvider interface:64` |
| TopologyHint 结构体 | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `TopologyHint struct:88` |
| NewManager 构造 | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `NewManager:119` |
| Scope 接口 | [scope.go](kubernetes/pkg/kubelet/cm/topologymanager/scope.go) | `Scope interface:39` |
| 容器粒度 Admit | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `Admit:44` |
| Pod 粒度 Admit | [scope_pod.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_pod.go) | `Admit:44` |
| Hint 收集 | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `accumulateProvidersHints:68` |
| 亲和性计算 | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `calculateAffinity:80` |

### 为什么需要 TopologyManager

现代服务器是多 NUMA 架构：CPU 0-7 与 Socket 0 的本地内存直连，访问 Socket 1 的内存要经过 Intersocket 总线，延迟翻倍。GPU/NIC 也通过 PCIe 与某个 socket 亲和。如果一个需要 GPU 和大量内存的容器，GPU 在 Socket 0 但内存分配在 NUMA Node 1，跨 NUMA 内存访问性能下降明显。

TopologyManager 的职责：**在 Pod 准入阶段，把 CPU、内存、设备的 NUMA 亲和性 Hints 汇总，计算出一个所有资源都满足的最优 NUMA 节点集合，然后让各管理器按这个亲和性分配资源**。

### 四种策略

| 策略 | 含义 |
|------|------|
| `none` | 不做 NUMA 亲和性约束，各管理器自由分配（默认） |
| `best-effort` | 尽量满足亲和性，但 Preferred=false 的容器也允许运行 |
| `restricted` | 必须满足亲和性，否则 Pod 准入失败 |
| `single-numa-node` | 所有资源必须在同一个 NUMA 节点，否则准入失败 |

### 两种 Scope

- **container scope**（默认）：每个容器独立计算亲和性，容器之间不互相感知
- **pod scope**：Pod 内所有容器合并计算亲和性，确保同一 Pod 的容器在同一 NUMA 节点上

### TopologyHint 数据结构

```go
// pkg/kubelet/cm/topologymanager/topology_manager.go:88
type TopologyHint struct {
    NUMANodeAffinity bitmask.BitMask  // 哪些 NUMA 节点可满足需求（位掩码）
    Preferred        bool             // 这个 Hint 是否是首选亲和性
}
```

`NUMANodeAffinity` 是位掩码，例如 `0b01` 表示 NUMA Node 0，`0b11` 表示 Node 0+1 均可。策略通过对多个 HintProvider 的 Hints 做交集（BitAnd），找到所有资源都能满足的最小 NUMA 节点集合。

### Admit 流程

```
manager.Admit(attrs)                       topology_manager.go:186
  │
  └─── scope.Admit(pod)                   — 按 scope 分发
          │
          ├─── 如果是 PolicyNone → admitPolicyNone() 直接放行
          │
          ├─── calculateAffinity(pod, container)
          │       │
          │       ├─── accumulateProvidersHints()   — 遍历所有 HintProvider，收集各自 Hints
          │       │       每个 Provider 返回 map[resourceName][]TopologyHint
          │       │
          │       └─── policy.Merge(hints)          — 按策略合并 Hints 取最佳亲和性
          │               最终得到 bestHint (NUMANodeAffinity + Preferred)
          │
          ├─── 如果 policy 拒绝 → GetPodAdmitResult(TopologyAffinityError)
          │
          └─── allocateAlignedResources(pod, container, bestHint)
                  └─── 遍历 hintProviders，调用 provider.Allocate(pod, container)
                       各 provider 按 bestHint.NUMANodeAffinity 约束分配资源
```

---

## §02 TopologyManager 源码解读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 顶层 Admit | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `Admit:186` |
| AddHintProvider | [topology_manager.go](kubernetes/pkg/kubelet/cm/topologymanager/topology_manager.go) | `AddHintProvider:174` |
| container scope Admit | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `Admit:44` |
| Hint 汇聚 | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `accumulateProvidersHints:68` |
| 亲和性计算 | [scope_container.go](kubernetes/pkg/kubelet/cm/topologymanager/scope_container.go) | `calculateAffinity:80` |

### scope 设计

TopologyManager 内部有两层抽象：

```
Manager（顶层）
  │
  └─── Scope（接口，scope.go:39）
          ├─── containerScope  — 每个容器独立走一遍 Hint 收集 + 合并 + 分配
          └─── podScope        — 聚合 Pod 内所有容器的 Hints 再统一分配
```

`Manager.Admit()` 直接委托给 `scope.Admit()`，自身只负责将 `attrs.Pod` 传进去并处理结果。这样新增 Scope 类型（如 namespace scope）不需要修改顶层 Manager。

### accumulateProvidersHints 与 calculateAffinity

```go
// scope_container.go:68  — 收集所有 HintProvider 的 Hints
func (s *containerScope) accumulateProvidersHints(pod *v1.Pod, container *v1.Container) []map[string][]TopologyHint {
    // 遍历 s.hintProviders（cpuMgr、memMgr、deviceMgr 都在里面）
    // 每个 provider 返回 map[resourceName][]TopologyHint
    // 结果是一个 slice，每个元素对应一个 provider 的 Hints map
}

// scope_container.go:80  — 将多个 Hints 合并为最优亲和性
func (s *containerScope) calculateAffinity(pod *v1.Pod, container *v1.Container) (TopologyHint, bool) {
    // 1. 调用 accumulateProvidersHints 收集所有 Hints
    // 2. 调用 policy.Merge(providersHints) 做 BitAnd 交集
    // 3. 返回 bestHint（Preferred=true 且 NUMANodeAffinity 最小的那个）
}
```

Merge 的核心逻辑：对每种资源的所有可能 Hints 做笛卡尔积，然后对每个组合做 BitAnd，得到候选集；在候选集里选 Preferred=true 且 NUMANodeAffinity 位数最少（最紧凑）的那个作为 bestHint。

---

## §03 写 Go 代码体会 cpuset 原理

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| CPU 拓扑发现 | [topology/topology.go](kubernetes/pkg/kubelet/cm/cpumanager/topology/topology.go) | `Discover:219` |

### cpuset 是什么

Linux 内核通过 cgroup `cpuset` 子系统控制进程可以运行在哪些 CPU 核心上。与 `cpu.cfs_quota_us` 的"限速"不同，`cpuset` 是**独占绑定**：进程只能运行在指定的 CPU 上，不会被调度到其他 CPU，消除了跨 CPU 的缓存迁移开销。

### Go 代码实验

课程通过 Go 程序演示了 cpuset 绑定效果：

```go
// 启动 Go 程序后，通过 cgexec 限制只能使用 CPU 0
// cgexec -g cpuset:my_cpuset ./cpu_demo
// 与不限制相比，多核任务的耗时从 2s 变为 21s（单核串行）
```

关键洞察：`cpuset` 限制的不是 CPU 时间片（那是 `cfs_quota`），而是**调度器选择哪个物理核心**。Guaranteed Pod 绑定独占核后，其他进程不会被调度到这些核，消除了 CPU 共享带来的上下文切换和缓存污染。

### CPUTopology 结构

kubelet 启动时通过 `cadvisor` 采集机器信息，`topology.Discover()` 将其转化为 `CPUTopology`：

```
CPUTopology
  ├─── NumCPUs        总逻辑 CPU 数（包含超线程）
  ├─── NumCores       总物理核数
  ├─── NumSockets     socket 数量（每个 socket 对应一个 NUMA 节点）
  └─── CPUDetails     map[cpuID → {NUMANodeID, SocketID, CoreID}}
```

这个拓扑信息是 `takeByTopology` 算法的输入，决定了 CPU 分配时如何优先填满整 socket/整 core。

---

## §04 kubelet 中的 cpuManager 解读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口 | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `Manager interface:54` |
| manager 结构体 | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `manager struct:93` |
| NewManager | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `NewManager:143` |
| Start — 启动周期同步 | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `Start:198` |
| AddContainer | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `AddContainer:248` |
| reconcileState | [cpu_manager.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_manager.go) | `reconcileState:361` |
| staticPolicy.Allocate | [policy_static.go](kubernetes/pkg/kubelet/cm/cpumanager/policy_static.go) | `Allocate:218` |
| allocateCPUs | [policy_static.go](kubernetes/pkg/kubelet/cm/cpumanager/policy_static.go) | `allocateCPUs:257` |
| takeByTopology | [cpu_assignment.go](kubernetes/pkg/kubelet/cm/cpumanager/cpu_assignment.go) | `takeByTopology:149` |

### 局部调用链

```
cpuManager.Allocate(pod, container)         cpu_manager.go
  │
  └─── staticPolicy.Allocate(state, pod, container)   policy_static.go:218
          │
          ├─── 判断 QoS != Guaranteed → return nil（不绑定，走共享池）
          ├─── 检查 CPU 请求必须是整数
          ├─── s.GetCPUSetOrDefault() → 已分配过则复用
          │
          └─── allocateCPUs(state, numCPUs, numaAffinity, reusableCPUs)   :257
                  │
                  ├─── GetAllocatableCPUs().Union(reusableCPUs)  — 可用池
                  │
                  ├─── 如果有 numaAffinity → takeByTopology(topo, aligned, numCPUs)  — NUMA 感知优先
                  │
                  └─── takeByTopology(topo, available, numCPUs)  cpu_assignment.go:149
                          │
                          算法三步（拓扑感知 best-fit）：
                          1. takeFullSockets() — 优先填满整个 socket
                          2. takeFullCores()   — 再填满整个 core（避免超线程干扰）
                          3. takeRemainingCPUs()— 最后取散列线程

cpuManager.AddContainer(pod, container, containerID)   cpu_manager.go:248
  │
  └─── 更新 containerMap（podUID → containerName → containerID）
       将分配好的 cpuset 通过 container runtime 写入容器的 cgroup cpuset

cpuManager.Start()   cpu_manager.go:198
  │
  └─── go wait.Until(reconcileState, reconcilePeriod, ...)   — 周期 goroutine
          │
          └─── reconcileState()   cpu_manager.go:361
                  │
                  ├─── 遍历 activePods 获取 podStatus
                  ├─── 遍历容器，获取容器 ID
                  ├─── 跳过 Waiting/Terminated 状态的容器
                  ├─── GetCPUSetOrDefault() 取 stateMemory 中存储的 cpuset
                  ├─── GetCPUSetOrDefault 取 lastUpdateState 上次同步的 cpuset
                  └─── 如果不一致 → updateContainerCPUSet() 调用 runtime 更新
```

### stateMemory 结构

```go
// pkg/kubelet/cm/cpumanager/state/state_mem.go
type stateMemory struct {
    sync.RWMutex
    assignments    ContainerCPUAssignments  // map[podUID][containerName]cpuset.CPUSet
    defaultCPUSet  cpuset.CPUSet
}
```

`assignments` 是双层 map：podUID → containerName → cpuset。`SetCPUSet` 写入时加锁，`GetCPUSet` 读时加读锁，reconcileState 以它为 source of truth。

### 为什么需要 reconcileState

kubelet 重启、容器运行时崩溃恢复后，cgroup 里的 cpuset 设置可能丢失。`reconcileState` 周期比对 stateMemory 与实际 cpuset，不一致时重写，保证绑定的 CPU 不因各种异常而失效。

### kubelet CPU 三分法

```
节点所有 CPU
  ├─── system-reserved / kube-reserved   — OS 和 kubelet 组件用
  ├─── eviction-hard（保留缓冲）
  ├─── Guaranteed 容器独占 cpuset        — staticPolicy 分配
  └─── 共享池（shared pool）             — BestEffort + Burstable 容器用 cfs 调度
```

---

## §05 memoryManager 原理简介

### 为什么需要 memoryManager

NUMA 架构下，进程访问本地 NUMA 节点的内存（local memory）远快于访问远端节点的内存（remote memory）。当进程使用内存达到本地 NUMA 节点上限时，内核会从远端节点分配，带来性能下降。

memoryManager 的目标：**为内存性能极高要求的容器（Guaranteed Pod）预分配指定 NUMA 节点的内存，避免跨 NUMA 访问**。

### 工作流程

```
                 Kubelet → TopologyManager → MemoryManager → MemoryMap
准入阶段：
  Kubelet.Admit()
    → TopologyManager.GetTopologyHints()   — 收集 memoryManager 的 Hint
        → MemoryManager.CalculatesAffinity — 基于 MemoryMap 计算可用 NUMA 节点
        → 返回 Hint 给 TopologyManager
    → TopologyManager.Allocate()
        → MemoryManager.Allocate()         — 更新 MemoryMap（记录分配）

容器创建阶段：
  Kubelet.PreCreateContainer()
    → MemoryManager.GetContainerMemoryAllocation() — 取已分配的 NUMA 内存信息
    → 写入容器 CRI 配置（numaMemoryAffinity）
```

### 配置方式

必须同时设置两个参数：

```bash
kubelet --memory-manager-policy=Static \
        --reserved-memory="<numaNodeID>:<resourceName>=<quantity>"
```

`--reserved-memory` 的约束：
- 每个 NUMA 节点预留内存 > 0
- 所有 NUMA 节点预留总量 ≥ `kube-reserved + system-reserved + eviction-hard`

### 当前三个限制（v1.21 alpha 阶段）

1. **无法同时支持单个和跨 NUMA 节点分配**：一个容器的内存跨两个 NUMA 节点时，无法知道从哪个节点实际消耗内存。
2. **只能为 Guaranteed Pod 服务**：Burstable Pod 可能随机从任何 NUMA 节点分配内存，这会干扰 Guaranteed Pod 分配后的 OOM 顺序。
3. **可能产生内存碎片**：Pod 和容器启停顺序不同会在 NUMA 节点上留下碎片，alpha 阶段没有整理机制。

---

## §06 memoryManager 源码阅读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口 | [memory_manager.go](kubernetes/pkg/kubelet/cm/memorymanager/memory_manager.go) | `Manager interface:55` |
| manager 结构体 | [memory_manager.go](kubernetes/pkg/kubelet/cm/memorymanager/memory_manager.go) | `manager struct:88` |
| NewManager | [memory_manager.go](kubernetes/pkg/kubelet/cm/memorymanager/memory_manager.go) | `NewManager:123` |
| AddContainer | [memory_manager.go](kubernetes/pkg/kubelet/cm/memorymanager/memory_manager.go) | `AddContainer:180` |
| RemoveContainer | [memory_manager.go](kubernetes/pkg/kubelet/cm/memorymanager/memory_manager.go) | `RemoveContainer:224` |
| staticPolicy.Allocate | [policy_static.go](kubernetes/pkg/kubelet/cm/memorymanager/policy_static.go) | `Allocate:87` |

### Manager 接口关键方法

```go
// memory_manager.go:55
type Manager interface {
    Start(activePods ActivePodsFunc, sourcesReady config.SourcesReady,
          podStatusProvider status.PodStatusProvider, containerRuntime runtimeService,
          initialContainers containermap.ContainerMap) error

    AddContainer(*v1.Pod, *v1.Container, string) error     // 容器启动后更新 containerMap
    RemoveContainer(containerID string) error               // 容器退出后释放内存

    // 向 TopologyManager 提供 NUMA 亲和性 Hints
    GetTopologyHints(*v1.Pod, *v1.Container) map[string][]topologymanager.TopologyHint
    GetPodTopologyHints(*v1.Pod) map[string][]topologymanager.TopologyHint

    // 供 kubelet 分配 / 释放内存
    Allocate(*v1.Pod, *v1.Container) error

    // 获取分配给容器的 NUMA 内存块信息（PreCreateContainer 时使用）
    GetMemoryNUMANodes(*v1.Pod, *v1.Container) sets.Int
    GetAllocatableMemory() []state.Block
    GetMemory(podUID, containerName string) []state.Block
}
```

### manager 结构体关键字段

```go
// memory_manager.go:88
type manager struct {
    sync.Mutex
    policy    Policy              // none 或 static

    // state 存储 Guaranteed pods 内存分配信息，kubelet 重启后可从此恢复
    state state.State

    containerRuntime runtimeService
    activePods ActivePodsFunc

    // 容器运行时容器 ID 到 Pod/Container 的映射
    containerMap containermap.ContainerMap

    // allocatableMemory 每个 NUMA node 已分配的内存
    allocatableMemory []state.Block

    // pendingAdmissionPod 准入阶段暂存的 Pod
    pendingAdmissionPod *v1.Pod
}
```

### NewManager 初始化流程

```go
// memory_manager.go:123
func NewManager(policyName string, machineInfo *cadvisorapi.MachineInfo,
    nodeAllocatableReservation v1.ResourceList,
    reservedMemory []kubeletconfig.MemoryReservation, ...) (Manager, error) {

    switch policyType(policyName) {
    case policyTypeNone:
        policy = NewPolicyNone()    // 即使 None 也需要 memoryManager 做容器内存分配和清理

    case policyTypeStatic:
        systemReserved = getSystemReservedMemory(machineInfo, nodeAllocatableReservation)
        policy = NewPolicyStatic(machineInfo, systemReserved, affinity)
    }
    manager := &manager{policy: policy, ...}
}
```

关键洞察：**即使不开启 Static 策略（使用 None），memoryManager 依然存在**，负责容器的内存分配记录和清理工作，只是不做 NUMA 亲和性优化。

### staticPolicy.Allocate 流程

```
staticPolicy.Allocate(state, pod, container)   policy_static.go:87
  │
  ├─── 只为 Guaranteed Pod 分配 → v1qos.GetPodQOS(pod) != Guaranteed → return nil
  │
  ├─── GetMemoryBlocks(podUID, containerName) → 已有缓存 → updatePodReusableMemory → return nil
  │
  ├─── 调用 TopologyManager 获取 bestHint：
  │       p.affinity.GetAffinity(podUID, containerName)
  │
  ├─── 获取容器请求的内存资源（requestedResources）
  │
  ├─── 从本地内存状态中获取 node 的 NUMA 状态（GetMachineState）
  │
  ├─── 如果 hint.NUMANodeAffinity == nil → getDefaultHint() 使用默认亲和性
  │
  ├─── 如果 hint 不能完全满足容器请求 → extendTopologyManagerHint() 扩展亲和性
  │
  ├─── 构造 containerBlocks（NUMANodeAffinity BitMask + resourceName + size）
  │       for resourceName, requestedSize := range requestedResources:
  │           containerBlocks = append(containerBlocks, state.Block{
  │               NUMANodeAffinity: maskBits,
  │               Size:             requestedSize,
  │               Type:             resourceName,
  │           })
  │           p.updateMachineState(machineState, maskBits, resourceName, requestedSize)
  │
  └─── 持久化：
        p.updatePodReusableMemory(pod, container, containerBlocks)
        s.SetMachineState(machineState)
        s.SetMemoryBlocks(podUID, containerName, containerBlocks)
```

### 容器启动：AddContainer 和 RemoveContainer

**AddContainer**（memory_manager.go:180）：
- 更新 `containerMap`（记录 containerID → podUID + containerName）
- 删除 init 容器的内存信息引用：因为每个 init 容器在下一个容器启动前已运行完毕，可以安全地释放其引用，从这些 init 容器中释放内存

**RemoveContainer**（memory_manager.go:224）：
- `GetMemoryBlocks(podUID, containerName)` 获取容器申请的内存 block 信息
- 遍历 blocks，遍历 NUMA node ID，`--NumberOfAssignments`
- 如果 `Reserved < releasedSize`：先释放 Reserved 那么多，剩余移到下一个 node
- 正常释放：`Free += releasedSize`，`Reserved -= releasedSize`
- 更新机器内存状态 `s.SetMachineState(machineState)`

### 容器启动 PreStartContainer 的三位一体

```go
// kuberuntime_container.go
func (i *internalContainerLifecycleImpl) PreStartContainer(pod, container, containerID) error {
    if i.cpuManager != nil {
        i.cpuManager.AddContainer(pod, container, containerID)     // 更新 containerMap
    }
    if i.memoryManager != nil {
        i.memoryManager.AddContainer(pod, container, containerID)  // 清理 init 容器引用
    }
    if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.TopologyManager) {
        i.topologyManager.AddContainer(pod, container, containerID)
    }
    return nil
}
```

---

## §07 device plugins 设备插件机制介绍

### 为什么需要设备插件框架

Kubernetes 核心代码不可能内置所有硬件（GPU、FPGA、RDMA NIC、InfiniBand 适配器等）的支持。设备插件框架（Device Plugin Framework）允许**硬件供应商以插件形式，无需修改 Kubernetes 代码，将硬件设备发布到 kubelet**。

### 核心约束

- 扩展资源**只能作为整数使用**，不支持小数（不能请求 0.5 个 GPU）
- 扩展资源**不能在容器之间共享**，一个设备同一时刻只能分配给一个容器

### Pod YAML 示例

```yaml
# 请求 2 个 hardware-vendor.example/foo 设备
spec:
  containers:
    - name: demo-container-1
      image: k8s.gcr.io/pause:2.0
      resources:
        limits:
          hardware-vendor.example/foo: 2  # 只能是整数
```

### 设备插件工作流程

设备插件需实现的 gRPC 服务接口：

```go
service DevicePlugin {
    rpc ListAndWatch(Empty) returns (stream ListAndWatchResponse) {}  // 持续上报设备列表
    rpc Allocate(AllocateRequest) returns (AllocateResponse) {}       // 分配设备给容器
    rpc GetDevicePluginOptions(Empty) returns (DevicePluginOptions) {}
    rpc PreStartContainer(PreStartContainerRequest) returns (...) {}  // 可选
    rpc GetPreferredAllocation(PreferredAllocationRequest) returns (...) {}
}
```

**完整生命周期**：
1. 设备插件启动，实现 ListAndWatch/Allocate 等 gRPC 服务
2. 通过 Unix socket `/var/lib/kubelet/device-plugins/kubelet.sock` 向 kubelet 注册自身
3. 注册成功后，设备插件进入监控模式，持续监控设备状态，向 kubelet 报告变化
4. 在 `Allocate` 期间，设备插件可能做设备初始化（如 GPU 清理或 QRNG 初始化）
5. 分配成功后，插件返回 `AllocateResponse`（环境变量、挂载点、设备文件等），kubelet 将这些信息传递给容器运行时

**kubelet 重启处理**：
- 设备插件能监测到 kubelet 重启，并向新的 kubelet 实例重新注册自己
- kubelet 重启时会删除 `/var/lib/kubelet/device-plugins` 下所有已有的 Unix socket
- 设备插件监控这个目录，发现 socket 消失时重新注册

### 设备插件与 TopologyManager 集成

设备插件可以在设备注册时填充 `TopologyInfo` 结构体，声明设备的 NUMA 亲和性：

```go
pluginapi.Device{
    ID:       "25102017",
    Health:   pluginapi.Healthy,
    Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 1}}},
}
```

这样 deviceManager 在 `GetTopologyHints()` 时能告知 TopologyManager 设备在哪个 NUMA 节点，实现 GPU + CPU + 内存的联合 NUMA 亲和性对齐。

---

## §08 deviceManager 源码解读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| ManagerImpl 结构体 | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `ManagerImpl struct:63` |
| NewManagerImpl | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `NewManagerImpl:127` |
| Start — 启动 gRPC Server | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `Start:241` |
| Register（device plugin 注册入口） | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `Register:413` |
| addEndpoint（建立长连接） | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `addEndpoint:485` |
| Allocate（准入时分配） | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `Allocate:369` |
| RegisterPlugin（插件注册回调） | [manager.go](kubernetes/pkg/kubelet/cm/devicemanager/manager.go) | `RegisterPlugin:319` |

### 局部调用链

```
deviceManager.Start()           manager.go:241
  │
  ├─── m.readCheckpoint()       — 从磁盘恢复 allocatedDevices / registeredDevices
  │       读取 /var/lib/kubelet/device-plugins/kubelet_internal_checkpoint
  │       遍历 checkpoint 条目，重新初始化 Endpoint（endpoints.run() 重连 device plugin）
  │
  ├─── 绑定 kubelet.sock       — 监听 Unix socket
  │
  └─── grpc.NewServer()         — 注册 Register gRPC 服务，等待 device plugin 调用

device plugin 调用 Register()   manager.go:413
  │
  ├─── 校验 ResourceName 格式（必须是 vendor-domain/resourceType，如 nvidia.com/gpu）
  │
  └─── addEndpoint(r)          manager.go:485
          │
          ├─── newEndpointImpl(socketPath, resourceName, callback)
          │       dial(socketPath) 建立到 device plugin 的 gRPC 连接
          │
          ├─── m.registerEndpoint(r.ResourceName, r.Options, new)
          │
          └─── go endpoint.run()   — goroutine：持续调用 ListAndWatch gRPC stream
                  │
                  ├─── 从 stream 获取 devices，调用 e.callback(resourceName, newDevs)
                  │       callback 即 genericDeviceUpdateCallback
                  │       → 更新 healthyDevices / unhealthyDevices / allDevices map
                  │
                  └─── 如果 ListAndWatch 出错 → stop() 关闭连接，设置 stopTime

准入阶段 deviceManager.Allocate()   manager.go:369
  │
  ├─── 遍历 pod.Spec.InitContainers + pod.Spec.Containers
  │
  └─── m.allocateContainerResources(pod, &container, m.devicesToReuse[...])
          │
          ├─── devicesToReuse 判断：复用 Pod 之前已分配但被 init 容器"占用"的 devices
          │
          └─── 对每种资源：m.allocateContainerResources()
                  │
                  └─── 调用 device plugin 的 Allocate gRPC
                        获取 AllocateResponse（设备文件路径、环境变量、挂载点）
                        m.podDevices.addContainerAllocatedResources(podUID, container.Name, ...)
```

### ManagerImpl 关键字段

```go
// manager.go:63
type ManagerImpl struct {
    socketname   string          // kubelet.sock 路径
    socketdir    string

    allDevices       ResourceDeviceInstances  // 所有已注册设备（含 unhealthy）
    healthyDevices   map[string]sets.String   // 健康设备 ID 集合，按资源名索引
    unhealthyDevices map[string]sets.String
    allocatedDevices map[string]sets.String   // 已分配设备

    endpoints     map[string]endpointInfo    // resourceName → Endpoint（与 device plugin 的长连接）

    numaNodes     []int
    topologyAffinityStore topologymanager.Store

    podDevices       *podDevices             // pod → container → 已分配设备
    devicesToReuse   PodReusableDevices
    checkpointManager checkpointmanager.CheckpointManager
    pendingAdmissionPod *v1.Pod
}
```

### Register gRPC 接口（对外暴露给 device plugin）

```go
// manager.go:413
func (m *ManagerImpl) Register(ctx context.Context, r *pluginapi.RegisterRequest) (*pluginapi.Empty, error) {
    // 校验 ResourceName 格式：必须包含 "/" 且不是 kubernetes.io 域
    if !v1helper.IsExtendedResourceName(v1.ResourceName(r.ResourceName)) {
        return nil, fmt.Errorf("invalid resource name from device plugin %q", r.ResourceName)
    }
    // 调用 addEndpoint，建立到 device plugin 的反向长连接
    go m.addEndpoint(r)
    return &pluginapi.Empty{}, nil
}
```

设计要点：kubelet 暴露 `Register` 给 device plugin 调用（注册方向：device plugin → kubelet），然后 kubelet 反过来连接 device plugin 的 socket 调用 `ListAndWatch`（监听方向：kubelet → device plugin）。这是一个**双向 gRPC** 架构。

### checkpoint 机制

deviceManager 将 `allocatedDevices`、`registeredDevices`、`podDevices` 等状态持久化到：

```
/var/lib/kubelet/device-plugins/kubelet_internal_checkpoint
```

kubelet 重启时 `readCheckpoint()` 恢复这些状态，重新建立与已注册 device plugin 的连接，保证 Pod 的设备分配不因 kubelet 重启而丢失。
