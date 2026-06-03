# 第12章 kubelet pleg 对象和 containerManager 总结

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 12 章 — PLEG 与 ContainerManager
> **源码入口**: `pkg/kubelet/pleg/generic.go`、`pkg/kubelet/cm/container_manager_linux.go`

---

## 核心机制一览

1. **PLEG 是 kubelet 的容器状态感知引擎**：通过定时执行 `relist()` 调用容器运行时获取所有容器状态，与内部缓存（podRecords）对比后生成事件，推送给 syncLoop 处理——而不是让每个 Pod worker 各自轮询容器运行时，大幅减少 CRI 调用次数。

2. **relist 的核心是双快照对比**：维护 old（上次）和 current（本次）两个 Pod 快照，遍历两者中所有容器，用 `computeEvents` 判断每个容器 ID 的状态变迁，由 `generateEvents` 生成对应事件（Started/Died/Removed/Unknown）。

3. **PLEG Healthy 判断依据是 relist 间隔**：`Healthy()` 检查距上次 relist 时间是否超过 3 分钟（`relistThreshold`）；kubelet syncLoop 定期调用 `runtimeState.runtimeErrors()` 遍历所有 healthCheck，PLEG 健康失败会导致 Pod 跳过同步（exponential backoff）。

4. **containerManager 是其他所有资源管理器的总管**：自身管理 cgroup、QoS cgroup、local ephemeral storage，并在 `NewContainerManager` 中依次初始化并持有 topologyManager、deviceManager、cpuManager、memoryManager，通过 `Start()` 统一启动。

5. **containerManager.Start() 五步初始化顺序**：①启动 cpuManager（reconcileState goroutine）→ ②启动 memoryManager（读 checkpoint 恢复状态）→ ③启动 deviceManager（监听 kubelet.sock）→ ④validateNodeAllocatable 校验配置 → ⑤setupNode（创建 QoS cgroup、检查 docker cgroup）。

6. **local ephemeral storage 的 Eviction 闭环**：kubelet 通过 `LocalStorageCapacityIsolation` 特性限制容器临时存储用量（emptyDir volumes、容器日志、image layers）；当容器用量超过 `limits.ephemeral-storage` 时触发 Pod Evict，controller 重建新 Pod，形成完整的存储保护闭环。

---

## 全章调用链总图

```
kubelet.New()
  │
  ├─── pleg.NewGenericPLEG(runtime, channelCapacity, relistPeriod, cache, clock)
  │       generic.go:110
  │       └─── 返回 GenericPLEG（podRecords, eventChannel, podCache）
  │
  ├─── klet.runtimeState.addHealthCheck("PLEG", klet.pleg.Healthy)
  │       注册 PLEG 健康检查到 runtimeState
  │
  └─── NewContainerManager(...)
          container_manager_linux.go:214
          │
          ├─── GetCgroupSubsystems()  — 解析 /proc/self/mountinfo 获取 cgroup 子系统
          ├─── initializeCgroupRoot() — 初始化 cgroupRoot
          ├─── NewQOSContainerManager()
          ├─── topologymanager.NewManager()
          ├─── devicemanager.NewManagerImpl()  → topologyManager.AddHintProvider()
          ├─── cpumanager.NewManager()         → topologyManager.AddHintProvider()
          └─── memorymanager.NewManager()      → topologyManager.AddHintProvider()

kubelet.Run()
  │
  ├─── kl.pleg.Start()                        generic.go:130
  │       └─── go wait.Until(g.relist, g.relistPeriod, wait.NeverStop)
  │
  └─── containerManager.Start()               container_manager_linux.go:611
          │
          ├─── cpuManager.Start()
          ├─── memoryManager.Start()
          ├─── deviceManager.Start()
          ├─── cm.validateNodeAllocatable()
          └─── cm.setupNode(activePods)        container_manager_linux.go:465
                  ├─── validateSystemRequirements()  — 检查 cpu/memory cgroup 已挂载
                  └─── createNodeAllocatableCgroups() — 创建顶级 QoS cgroup

syncLoop（持续运行）
  │
  ├─── kl.pleg.Watch()  — 返回 plegCh channel
  │
  └─── syncLoopIteration(updates, handler, syncTicker.C, housekeepingTicker.C, plegCh)
          │
          ├─── case e := <-plegCh:
          │       ├─── ContainerStarted → kl.lastContainerStartedTime.Add(e.ID, now)
          │       ├─── 非 remove → isSyncPodWorthy(e) → handler.HandlePodSyncs([pod])
          │       │       kubelet.go:2180  → dispatchWork → syncPod
          │       └─── ContainerDied → kl.cleanUpContainersInPod(podID, containerID)
          │               kubelet.go:2275
          │
          └─── relist() 定期执行：
                  generic.go:190
                  ├─── metrics 记录（relistInterval / relistDuration）
                  ├─── g.runtime.GetPods(true)  — 调用容器运行时获取所有 Pod
                  ├─── updateRunningPodAndContainerMetrics(pods)
                  ├─── g.podRecords.setCurrent(pods)  — 更新当前快照
                  ├─── for pid := range g.podRecords:
                  │       allContainers := getContainersFromPods(oldPod, newPod)
                  │       for container in allContainers:
                  │           events := computeEvents(oldPod, newPod, &container.ID)  :333
                  │               oldState := getContainerState(oldPod, cid)          :410
                  │               newState := getContainerState(newPod, cid)
                  │               return generateEvents(pid, cid, oldState, newState) :150
                  │           → updateEvents(eventsByPodID, e)
                  └─── 发送事件到 g.eventChannel（供 plegCh 消费）
```

---

## §01 kubelet pleg 对象介绍和源码解读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| PLEG 接口定义 | [pleg.go](kubernetes/pkg/kubelet/pleg/pleg.go) | `PodLifecycleEventGenerator:53` |
| GenericPLEG 结构体 | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `GenericPLEG:49` |
| NewGenericPLEG 构造 | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `NewGenericPLEG:110` |
| Start — 启动周期 relist | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `Start:130` |
| Healthy — 健康检查 | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `Healthy:136` |
| relist — 核心状态采集 | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `relist:190` |
| generateEvents | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `generateEvents:150` |
| computeEvents | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `computeEvents:333` |
| getContainerState | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `getContainerState:410` |
| convertState | [generic.go](kubernetes/pkg/kubelet/pleg/generic.go) | `convertState:86` |

### 为什么需要 PLEG

在 Kubernetes 早期，kubelet 中每个 Pod 都有一个独立 goroutine，这些 goroutine 定时（约 10s）轮询容器状态。当节点上 Pod 数量多时，并发的容器运行时调用量极大，给 container runtime 带来沉重负担，导致性能问题。

PLEG 的解法：**将所有 Pod 的容器状态采集统一到一个 goroutine 里做，一次 `relist` 调用获取所有 Pod 信息，然后通过事件分发给各 Pod worker，消除了 N 个 Pod × 轮询频率的并发调用**。

### PodLifecycleEventGenerator 接口

```go
// pkg/kubelet/pleg/pleg.go:53
type PodLifecycleEventGenerator interface {
    Start()
    Watch() chan *PodLifecycleEvent
    Healthy() (bool, error)
}
```

三个方法：`Start()` 启动定时 relist goroutine，`Watch()` 返回事件 channel（syncLoop 消费），`Healthy()` 供健康检查调用。

### GenericPLEG 结构体

```go
// pkg/kubelet/pleg/generic.go:49
type GenericPLEG struct {
    relistPeriod time.Duration       // relist 间隔，默认 1s
    runtime      kubecontainer.Runtime
    eventChannel chan *PodLifecycleEvent  // 容量 1000
    podRecords   podRecords           // old/current 双快照
    relistTime   atomic.Value         // 上次 relist 完成时间
    cache        kubecontainer.Cache  // Pod Cache，供其他组件读取容器状态
    clock        clock.Clock
}
```

`podRecords` 是核心数据结构，维护每个 Pod 的 old（上次快照）和 current（本次快照）。

### NewGenericPLEG 构造

```go
// pkg/kubelet/pleg/generic.go:110
func NewGenericPLEG(runtime kubecontainer.Runtime, channelCapacity int,
    relistPeriod time.Duration, cache kubecontainer.Cache,
    clock clock.Clock) PodLifecycleEventGenerator {
    return &GenericPLEG{
        relistPeriod: relistPeriod,
        runtime:      runtime,
        eventChannel: make(chan *PodLifecycleEvent, channelCapacity),
        podRecords:   make(podRecords),
        cache:        cache,
        clock:        clock,
    }
}
```

kubelet 初始化时传入的参数：`channelCapacity=1000`，`relistPeriod` 默认 1s，`podCache` 由 kubelet 统一持有供多处读取。

### PLEG Healthy 检查

```go
// pkg/kubelet/pleg/generic.go:136
func (g *GenericPLEG) Healthy() (bool, error) {
    relistTime := g.getRelistTime()
    if relistTime.IsZero() {
        return false, fmt.Errorf("pleg has yet to be successful")
    }
    elapsed := g.clock.Since(relistTime)
    if elapsed > relistThreshold {  // relistThreshold = 3min
        return false, fmt.Errorf("pleg was last seen active %v ago; threshold is %v", elapsed, relistThreshold)
    }
    return true, nil
}
```

**健康检查的传播链**：

```
kubelet.New()
  └─── klet.runtimeState.addHealthCheck("PLEG", klet.pleg.Healthy)

syncLoop()
  └─── for { if err := kl.runtimeState.runtimeErrors(); err != nil {
           // exponential backoff, 跳过 Pod 同步
       }
```

`runtimeState.runtimeErrors()` 遍历所有 `healthChecks`（包括 PLEG），任一失败则 syncLoop 进入 backoff，保护 kubelet 不在运行时不健康时继续大量创建/更新容器。

### relist 全流程

```
relist()    generic.go:190
  │
  ├─── 记录上次 relist 时间 → metrics.PLEGRelistInterval
  ├─── defer 记录本次 relist 耗时 → metrics.PLEGRelistDuration
  │
  ├─── g.runtime.GetPods(true)   — 调用 CRI 获取所有 Pod（含 sandbox + containers）
  │
  ├─── g.updateRelistTime(timestamp)   — 更新 relistTime（Healthy 检查的依据）
  │
  ├─── updateRunningPodAndContainerMetrics(pods)   — 更新 metrics（running pod 数、各状态容器数）
  │       遍历 pods 统计 containerStateCount（created/running/exited/unknown）
  │       统计 runningSandboxNum（每个 pod 只有一个 running sandbox）
  │
  ├─── g.podRecords.setCurrent(pods)   — 更新 current 快照
  │
  ├─── for pid := range g.podRecords:
  │       oldPod := g.podRecords.getOld(pid)
  │       pod    := g.podRecords.getCurrent(pid)
  │       allContainers := getContainersFromPods(oldPod, pod)
  │           // 去重合并 old+current 中的所有容器（Containers + Sandboxes）
  │       for _, container := range allContainers:
  │           events := computeEvents(oldPod, pod, &container.ID)   :333
  │           for _, e := range events:
  │               updateEvents(eventsByPodID, e)
  │
  ├─── 更新 podCache（通知订阅者）
  │
  ├─── g.podRecords.update(pid)   — old = current，current = nil（准备下次 relist）
  │
  └─── 发送事件到 g.eventChannel
          for pid, events := range eventsByPodID:
              for _, e := range events:
                  if e.Type == ContainerChanged { continue }
                  g.eventChannel <- e
```

### computeEvents 与 generateEvents

```go
// generic.go:333
func computeEvents(oldPod, newPod *kubecontainer.Pod, cid *kubecontainer.ContainerID) []*PodLifecycleEvent {
    oldState := getContainerState(oldPod, cid)   // :410 — 从 Pod 快照中找到该容器 ID 的状态
    newState := getContainerState(newPod, cid)
    return generateEvents(pid, cid.ID, oldState, newState)   // :150
}
```

`getContainerState` 逻辑：
- pod == nil → `plegContainerNonExistent`（容器不存在）
- 在 pod.FindContainerByID 找到 → `convertState(container.State)`
- 找不到 → `plegContainerNonExistent`

`convertState`（generic.go:86）将运行时状态映射为 pleg 内部状态：

| 运行时状态 | pleg 内部状态 |
|-----------|------------|
| ContainerStateCreated | plegContainerUnknown（kubelet 不用 created） |
| ContainerStateRunning | plegContainerRunning |
| ContainerStateExited | plegContainerExited |
| ContainerStateUnknown | plegContainerUnknown |

`generateEvents`（generic.go:150）根据 oldState → newState 生成事件：

| 变迁 | 生成事件 |
|------|---------|
| 任意 → Running | ContainerStarted |
| Running/Created/Unknown → Exited | ContainerDied |
| NonExistent → Exited | ContainerDied（已结束且没有 old 记录） |
| 任意 → NonExistent | ContainerRemoved |

### syncLoop 中消费 PLEG 事件

```go
// syncLoopIteration 中
plegCh := kl.pleg.Watch()

case e := <-plegCh:
    if e.Type == pleg.ContainerStarted {
        // 记录容器启动时间（用于 graceful termination 检测）
        kl.lastContainerStartedTime.Add(e.ID, time.Now())
    }
    if isSyncPodWorthy(e) {
        // 非 remove 事件：在 podManager 里查找 Pod，存在则通过 HandlePodSyncs 触发同步
        if pod, ok := kl.podManager.GetPodByUID(e.ID); ok {
            handler.HandlePodSyncs([]*v1.Pod{pod})   // kubelet.go:2180
        } else {
            // pod 不存在（已删除），忽略
        }
    }
    if e.Type == pleg.ContainerDied {
        // 触发容器清理
        kl.cleanUpContainersInPod(e.ID, containerID)  // kubelet.go:2275
    }
```

`HandlePodSyncs` 实质上调用 `dispatchWork`，把 Pod 投入到 podWorkers 队列中异步执行 `syncPod`。

---

## §02 kubelet containerManager 源码解读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| ContainerManager 接口 | [container_manager.go](kubernetes/pkg/kubelet/cm/container_manager.go) | `ContainerManager:46` |
| containerManagerImpl 结构体 | [container_manager_linux.go](kubernetes/pkg/kubelet/cm/container_manager_linux.go) | `containerManagerImpl:113` |
| NewContainerManager | [container_manager_linux.go](kubernetes/pkg/kubelet/cm/container_manager_linux.go) | `NewContainerManager:214` |
| Start | [container_manager_linux.go](kubernetes/pkg/kubelet/cm/container_manager_linux.go) | `Start:611` |
| setupNode | [container_manager_linux.go](kubernetes/pkg/kubelet/cm/container_manager_linux.go) | `setupNode:465` |

### ContainerManager 接口

```go
// pkg/kubelet/cm/container_manager.go:46
type ContainerManager interface {
    Start(*v1.Node, ActivePodsFunc, ...) error

    // 资源容量查询
    GetCapacity() v1.ResourceList
    GetNodeAllocatableReservation() v1.ResourceList
    GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string)

    // 资源分配管理
    UpdateAllocatedDevices()
    UpdatePluginResources(*schedulerframework.NodeInfo, *lifecycle.PodAdmitAttributes) error

    // 容器生命周期钩子
    InternalContainerLifecycle() InternalContainerLifecycle
    GetResources(pod, container) (*kubecontainer.RunContainerOptions, error)
}
```

三大职责：**资源容量查询**（供调度器做 Pod 分配决策）、**资源分配管理**（更新 device 分配状态）、**容器生命周期钩子**（容器启动前注入 CPU/内存/设备资源配置）。

### containerManagerImpl 关键字段

```go
// container_manager_linux.go:113
type containerManagerImpl struct {
    sync.RWMutex
    cadvisorInterface cadvisor.Interface
    mountUtil         mount.Interface
    NodeConfig                           // cgroup 路径、系统预留等配置
    status            Status
    systemContainers  []*SystemContainer
    subsystems        *CgroupSubsystems  // 已挂载的 cgroup 子系统
    cgroupManager     CgroupManager
    capacity          v1.ResourceList    // 节点总容量
    internalCapacity  v1.ResourceList    // 含内部资源
    cgroupRoot        CgroupName
    recorder          record.EventRecorder
    qosContainerManager QOSContainerManager
    deviceManager     devicemanager.Manager
    cpuManager        cpumanager.Manager
    memoryManager     memorymanager.Manager
    topologyManager   topologymanager.Manager
    // docker 特殊处理
    RuntimeCgroupsName string
    periodicTasks      []func()
}
```

`containerManagerImpl` 是资源管理器的聚合体，持有所有子管理器的引用，通过统一接口暴露给 kubelet。

### NewContainerManager 初始化序列

```
NewContainerManager()   container_manager_linux.go:214
  │
  ├─── GetCgroupSubsystems()        — 解析 /proc/self/mountinfo，确认 cgroup 子系统挂载点
  │
  ├─── ① swap 检查                  — 如果 failSwapOn=true 且 /proc/swaps 有 swap，返回错误
  │
  ├─── ② cadvisor 初始化            — 通过 cadvisor 获取节点容量（CPU、内存、存储）
  │       capacity = cadvisor.MachineInfo.Capacity
  │       （此时 cadvisor 尚未完全启动，只用于采集基础信息）
  │
  ├─── ③ initializeCgroupRoot()     — 从 cgroupsPath 字符串解析为 internal CgroupName
  │       校验 cgroup root 在所有子系统中是否存在
  │
  ├─── ④ NewQOSContainerManager()   — 初始化 QoS 顶级 cgroup 管理器
  │       （kubepods.slice → besteffort/burstable 两个子 cgroup）
  │
  ├─── ⑤ 构造 containerManagerImpl  — 填充所有基础字段
  │
  ├─── ⑥ 初始化 topologyManager     — Feature Gate: TopologyManager
  │       topologymanager.NewManager(machineInfo.Topology, policy, scope)
  │       若未开启：topologymanager.NewFakeManager()（noop 实现）
  │
  ├─── ⑦ 初始化 deviceManager       — Feature Gate: DevicePlugins
  │       devicemanager.NewManagerImpl(topology, topologyManager)
  │       cm.topologyManager.AddHintProvider(cm.deviceManager)
  │
  ├─── ⑧ 初始化 cpuManager          — Feature Gate: CPUManager
  │       cpumanager.NewManager(policy, reconcilePeriod, machineInfo, ...)
  │       cm.topologyManager.AddHintProvider(cm.cpuManager)
  │
  └─── ⑨ 初始化 memoryManager       — Feature Gate: MemoryManager
          memorymanager.NewManager(policy, machineInfo, reservedMemory, ...)
          cm.topologyManager.AddHintProvider(cm.memoryManager)
```

各子管理器都通过 `AddHintProvider` 注册到 topologyManager，使 Pod 准入时能统一做 NUMA 亲和性计算。

### containerManager.Start() 启动流程

```
containerManagerImpl.Start()    container_manager_linux.go:611
  │
  ├─── ① cpuManager.Start()
  │       — 读取已有状态（checkpoint）
  │       — 启动 reconcileState goroutine（周期同步 cpuset）
  │
  ├─── ② memoryManager.Start()
  │       — 读取 checkpoint 恢复分配状态
  │
  ├─── ③ deviceManager.Start()
  │       — 读取 checkpoint（allocatedDevices、registeredDevices）
  │       — 启动 kubelet.sock gRPC 服务，等待 device plugin 注册
  │
  ├─── ④ 限制 local ephemeral storage 容量
  │       if LocalStorageCapacityIsolation {
  │           rootfs := cm.cadvisorInterface.RootFsInfo()
  │           capacity["ephemeral-storage"] = rootfs 总容量
  │           // 通过 EphemeralStorageCapacityFromFsInfo() 换算
  │       }
  │
  ├─── ⑤ cm.validateNodeAllocatable()  — 校验 node allocatable 配置有效性
  │
  ├─── ⑥ cm.setupNode(activePods)     container_manager_linux.go:465
  │       │
  │       ├─── validateSystemRequirements(cm.mountUtil)
  │       │       — 检查 cpu、memory cgroup 子系统已挂载（cgroup v1）
  │       │
  │       ├─── if CgroupsPerQOS:
  │       │       cm.createNodeAllocatableCgroups()   — 创建 kubepods.slice
  │       │       cm.qosContainerManager.Start(...)   — 启动 QoS 顶级 cgroup 管理
  │       │
  │       └─── if ContainerRuntime == "docker":
  │               // 注册定期任务，检查 docker 进程的 cgroup
  │               cm.periodicTasks = append(cm.periodicTasks, func() {
  │                   cont := getContainerNameForProcess(dockerProcessName, dockerPidFile)
  │                   cm.RuntimeCgroupsName = cont
  │               })
  │
  └─── 启动 periodicTasks goroutine（cm.periodicTasks 里注册的周期任务）
```

### local ephemeral storage 机制

**背景**：kubelet 节点 `/var/lib/kubelet/` 下存放着容器的 emptyDir volumes、容器日志（`/var/lib/docker/`）、image layers（container writable layers）。这些都是"临时存储"，如果不加限制，单个容器可能写满节点磁盘，影响其他 Pod。

**工作原理**：`LocalStorageCapacityIsolation`（从 v1.10 起默认 beta 开启）通过 kubelet 定期检查每个容器的临时存储使用量，当超过 `limits.ephemeral-storage` 时触发 Pod Eviction。

```yaml
# Pod YAML 配置示例
resources:
  limits:
    ephemeral-storage: 2Gi
  requests:
    ephemeral-storage: 2Gi
```

**Eviction 闭环演示**：

```bash
# 进入容器，尝试写 4Gi 文件（超过 2Gi 限制）
dd if=/dev/zero of=/test bs=4096 count=1024000

# kubectl get pods 观察到：
nginx-75bf8666b8-89xqm   1/1   Running   0   1h
nginx-75bf8666b8-pm687   0/1   Evicted   0   2h   # 被驱逐
# controller 随即重建新 Pod
```

**容量获取**（container_manager_linux.go Start 内）：

```go
if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.LocalStorageCapacityIsolation) {
    rootfs, err := cm.cadvisorInterface.RootFsInfo()
    for rName, rCap := range cadvisor.EphemeralStorageCapacityFromFsInfo(rootfs) {
        cm.capacity[rName] = rCap
    }
}

// EphemeralStorageCapacityFromFsInfo
func EphemeralStorageCapacityFromFsInfo(info cadvisorapi2.FsInfo) v1.ResourceList {
    c := v1.ResourceList{
        v1.ResourceEphemeralStorage: *resource.NewQuantity(
            int64(info.Capacity), resource.BinarySI,
        ),
    }
    return c
}
```

`cadvisor.RootFsInfo()` 返回挂载了 `/var/lib/kubelet` 所在文件系统的总容量，作为节点临时存储容量上限写入 `cm.capacity`。

### containerManager 的全局角色

```
containerManager（kubelet 视角）
  │
  ├─── 向 kubelet 提供节点容量信息
  │       GetCapacity() → 调度器据此分配 Pod
  │       GetNodeAllocatableReservation() → 留给系统进程的资源
  │
  ├─── Pod 准入阶段
  │       InternalContainerLifecycle().PreStartContainer()
  │       → cpuManager.AddContainer / memoryManager.AddContainer / topologyManager.AddContainer
  │
  ├─── 容器运行配置
  │       GetResources(pod, container) → 返回 devices、env vars、mounts（来自 deviceManager）
  │
  └─── 各子管理器统一入口
          Start() 统一启动所有子管理器
          UpdateAllocatedDevices() → deviceManager.UpdateAllocatedDevices()
          UpdatePluginResources() → deviceManager.Allocate()
```
