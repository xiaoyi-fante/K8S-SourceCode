# 第09章 kubelet 稳定性保证：Eviction 驱逐与 OOM

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 9 章 — kubelet 稳定性保证 Eviction 驱逐和 OOM
> **源码入口**: `pkg/kubelet/eviction/eviction_manager.go`、`pkg/kubelet/oom/oom_watcher_linux.go`

---

## 核心机制一览

1. **两道防线**：kubelet 维护节点稳定性依靠两道机制协作——第一道是 evictionManager 主动驱逐 Pod 回收不可压缩资源；第二道是 Linux OOM Killer 在内核层面杀死进程。两道防线通过 `oom_score_adj` 联动：kubelet 在启动容器时按 QoS 设置 `oom_score_adj`，影响 OOM Killer 的选择优先级。

2. **evictionManager 同时扮演两个角色**：`NewManager` 返回同一个 `managerImpl` 实例，赋值给两个变量：`evictionManager`（做本机驱逐判定和执行）和 `evictionAdmitHandler`（在 kubelet 创建 Pod 前做准入检查，节点有压力时拦截新 Pod 调度进来）。

3. **内存驱逐阈值触发有两种路径**：优先走内核 memcg 通知机制（`notifyFunc`），内存一旦到达阈值立刻通知 kubelet，实时性高；若未开启则退化为轮询 `synchronize()`。

4. **synchronize 是核心判定函数**：每轮调用依次完成"采集信号 → 计算已触发阈值 → 合并未恢复阈值 → 按资源排序 Pod → 每轮驱逐一个 Pod"五步，每次驱逐间隔 sleep `monitoringInterval`。

5. **QoS 决定 OOMScoreAdj**：Guaranteed → -997，BestEffort → 1000，Burstable → 1000 − (1000 × memoryRequest / memoryCapacity)（下界 3，上界 999）。数值越大越优先被 OOM Killer 杀死，与 eviction 驱逐优先级方向一致。

6. **oomWatcher 生产者/消费者模型**：底层打开 `/dev/kmsg` 读取内核日志，Cadvisor 的 `oomparser` 作为生产者解析出 OOM 事件写入 channel，oomWatcher 消费者从 channel 读取后通过 `recorder.Eventf` 发出 K8s Warning Event。

---

## 全章调用链总图

```
kubelet.New()
  │
  ├─── eviction.NewManager(...)                          eviction_manager.go:107
  │      └─── 返回同一个 managerImpl 实例给
  │             ├─── evictionManager   (本机驱逐)
  │             └─── evictionAdmitHandler (准入检查)
  │
  ├─── oomwatcher.NewWatcher(recorder)                   oom_watcher_linux.go:47
  │      └─── oomparser.New() → 打开 /dev/kmsg
  │
  └─── kubelet.Run()
         │
         ├─── evictionManager.Start()                    eviction_manager.go:177
         │      ├─── [if memcg] 注册 notifyFunc 通知
         │      │      └─── 触发 → synchronize()
         │      └─── [else] goroutine 轮询 synchronize()
         │
         └─── oomWatcher.Start(ref)                      oom_watcher_linux.go:67
                ├─── goroutine: StreamOoms → outStream
                └─── goroutine: 消费 outStream → recorder.Eventf

synchronize()                                            eviction_manager.go:231
  │
  ├─── podFunc()          获取活跃 Pod
  ├─── summaryProvider.Get()   采集节点资源快照
  ├─── buildSignalToRankFunc() 注册各信号排序函数     helpers.go:948
  ├─── makeSignalObservations()  计算各信号观测值     helpers.go:659
  ├─── thresholdsMet()     计算已触发阈值集合
  ├─── mergeThresholds()   合并未恢复的历史阈值
  ├─── rank(activePods, statsFunc)  对 Pod 排序
  └─── for range activePods → killPodNow()             pod_workers.go:291
         └─── podWorkers.UpdatePod(SyncPodKill)
                └─── syncPod() → Phase=Failed
```

---

## §01 Kubelet Eviction 驱逐解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Eviction 策略概念 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `Start:177` |
| 驱逐信号定义 | [types.go](kubernetes/pkg/kubelet/eviction/types.go) | `Manager interface:56` |

**节点压力驱逐**是 kubelet 主动终止 Pod 以回收节点不可压缩资源（内存、磁盘、inode、PID）的过程。这在内存不足时尤为重要——内存无法像 CPU 那样通过限速降级，只能通过驱逐 Pod 来释放。

**驱逐信号**涵盖三类资源：

- **MemoryPressure**：内存压力
- **DiskPressure**：磁盘压力，kubelet 支持两种文件系统分区
  - `nodefs`：存储 volume 和 daemon logs
  - `imagefs`：容器镜像运行层（docker writable layer）
- **PIDPressure**：PID 数量压力

**kubelet 驱逐时 Pod 的选择优先级**：

1. 优先驱逐资源使用量超过其请求量的 BestEffort 或 Burstable Pod，根据优先级和超量程度排序
2. 资源使用量少于请求量的 Guaranteed Pod 和 Burstable Pod 根据优先级最后驱逐

**Kubelet Eviction Policy 的工作机制**：

- 检测到 Pod 的 `podPhase == Failed` 时触发后续清理
- kubelet 将 Pod 状态更新为 Failed，之后删除 Pod（若节点上 Pod 数超过限制）

---

## §02 EvictionManager 源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口定义 | [types.go](kubernetes/pkg/kubelet/eviction/types.go) | `Manager interface:56` |
| managerImpl 结构体 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `managerImpl:60` |
| evictionManager 初始化 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `NewManager:107` |
| 准入检查 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `Admit:137` |
| 监控启动 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `Start:177` |
| 核心判定 | [eviction_manager.go](kubernetes/pkg/kubelet/eviction/eviction_manager.go) | `synchronize:231` |
| 信号排序函数构建 | [helpers.go](kubernetes/pkg/kubelet/eviction/helpers.go) | `buildSignalToRankFunc:948` |
| 节点资源回收函数构建 | [helpers.go](kubernetes/pkg/kubelet/eviction/helpers.go) | `buildSignalToNodeReclaimFuncs:979` |
| 信号观测值计算 | [helpers.go](kubernetes/pkg/kubelet/eviction/helpers.go) | `makeSignalObservations:659` |

### evictionManager 初始化了两个相同的 manager 对象

`NewManager` 返回 `(Manager, lifecycle.PodAdmitHandler)`，但两者指向同一个 `*managerImpl`：

```go
// pkg/kubelet/kubelet.go:785
evictionManager, evictionAdmitHandler := eviction.NewManager(klet.resourceAnalyzer, evictionConfig,
    killPodNow(klet.podWorkers, kubeDeps.Recorder), ...)
```

- **evictionManager**：做本机驱逐 Pod 的判定和执行（`Start` + `synchronize`）
- **evictionAdmitHandler**：kubelet 创建 Pod 前，依据本机资源压力进行准入检查（`Admit`）

### evictionManager 接口定义

```go
// pkg/kubelet/eviction/types.go:56
type Manager interface {
    // Start 用来监控驱逐的阈值
    Start(diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc,
          podCleanedUpFunc PodCleanedUpFunc, monitoringInterval time.Duration)

    // 返回内存是否有压力
    IsUnderMemoryPressure() bool
    // 返回磁盘是否有压力
    IsUnderDiskPressure() bool
    // 返回 PID 是否有压力
    IsUnderPIDPressure() bool
}
```

### managerImpl 关键字段

```go
// pkg/kubelet/eviction/eviction_manager.go:60
type managerImpl struct {
    clock              clock.Clock
    config             Config          // 驱逐阈值配置
    killPodFunc        KillPodFunc     // 杀 Pod 的函数
    mirrorPodFunc      MirrorPodFunc
    imageGC            ImageGC
    containerGC        ContainerGC
    recorder           record.EventRecorder
    nodeRef            *v1.ObjectReference
    summaryProvider    stats.SummaryProvider   // 获取节点资源快照
    nodeConditions     []v1.NodeConditionType   // 当前 node condition 列表
    thresholdsMet      []evictionapi.Threshold  // 已触发的阈值
    dedicatedImageFs   *bool                    // imagefs 是否独立分区
    thresholdNotifiers []ThresholdNotifier      // memcg 通知器
}
```

### evictionManager 判断内存驱逐阈值有两种方法

- **第一种（优先）**：使用内核的 **memcg 通知机制**。当内存使用第一时间达到阈值，立刻通知 kubelet 并触发 `synchronize()`，实时性高。
- **第二种**：使用轮询检查机制，按 `monitoringInterval` 周期调用 `synchronize()`。

```go
// pkg/kubelet/eviction/eviction_manager.go:177（Start 函数节选）
if m.config.KernelMemcgNotification {
    for _, threshold := range thresholds {
        notifier, err := NewMemoryThresholdNotifier(threshold, m.config.PodCgroupRoot,
            &m.clock, m.synchronize)
        // ...
        go notifier.Start()
    }
} else {
    go func() {
        for {
            if evictedPods := m.synchronize(diskInfoProvider, podFunc); evictedPods != nil {
                klog.InfoS("Eviction manager: pods evicted, waiting for pod to be cleaned up")
                m.waitForPodsCleanup(podCleanedUpFunc, evictedPods)
            } else {
                time.Sleep(monitoringInterval)
            }
        }
    }()
}
```

### evictionAdmitHandler 的 Admit 方法

`Admit` 以 node 的压力状态作为 Pod 是否准入的依据，相当于在调度结果落地前再加一层门控：

```go
// pkg/kubelet/eviction/eviction_manager.go:137
func (m *managerImpl) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
    m.RLock()
    defer m.RUnlock()
    // node 无压力，直接准入
    if len(m.nodeConditions) == 0 {
        return lifecycle.PodAdmitResult{Admit: true}
    }
    // 静态 Pod 或 SystemCriticalPriority 高优先级，忽略节点压力
    if kubetypes.IsCriticalPod(attrs.Pod) {
        return lifecycle.PodAdmitResult{Admit: true}
    }
    // 当前节点只有内存压力时，非 BestEffort Pod 准入
    if nodeOnlyHasMemoryPressureCondition {
        notBestEffort := v1.PodQOSBestEffort != v1qos.GetPodQOS(attrs.Pod)
        if notBestEffort {
            return lifecycle.PodAdmitResult{Admit: true}
        }
        // BestEffort Pod：根据其 Toleration 判断
        if v1helper.TolerationsTolerateTaint(attrs.Pod.Spec.Tolerations,
            &v1.Taint{Key: v1.TaintNodeMemoryPressure, Effect: v1.TaintEffectNoSchedule}) {
            return lifecycle.PodAdmitResult{Admit: true}
        }
    }
    // 有 disk 压力，拒绝准入
    return lifecycle.PodAdmitResult{Admit: false, ...}
}
```

### synchronize 核心判定流程

```
synchronize()
  │
  ├─── 1. podFunc()                获取当前活跃 Pod 列表
  ├─── 2. summaryProvider.Get()   采集节点资源快照（内存/磁盘/PID）
  ├─── 3. makeSignalObservations() 从 summary 提取各信号的可用量/总量
  ├─── 4. thresholdsMet()         计算当前已触发的 threshold 集合
  ├─── 5. mergeThresholds()       合并之前已触发但尚未恢复的 threshold
  ├─── 6. nodeConditions()        将 threshold 转换为 NodeCondition
  ├─── 7. thresholdsFirstObservedAt() 过滤满足优雅删除的 threshold
  ├─── 8. thresholdsUpdatedAt()   过滤上次同步后状态有更新的 threshold
  ├─── 9. sort by eviction priority  按驱逐优先级排序 threshold
  ├─── 10. rank(activePods)       用 signalToRankFunc 对 Pod 排序
  └─── 11. for range activePods → 每轮驱逐一个 Pod
```

**makeSignalObservations** 为每个驱逐信号生成 `signalObservation`（可用量 + 总量）和 `statsFunc`（后续 Pod 排序用到的资源使用量方法）。

**buildSignalToRankFunc** 为每种信号注册对应的 Pod 排序函数，区分 `withImageFs`（imagefs 独立分区）两种场景：

```go
// pkg/kubelet/eviction/helpers.go:948
func buildSignalToRankFunc(withImageFs bool) map[evictionapi.Signal]rankFunc {
    signalToRankFunc := map[evictionapi.Signal]rankFunc{
        evictionapi.SignalMemoryAvailable:   rankMemoryPressure,
        evictionapi.SignalNodeFsAvailable:   rankDiskPressureFunc([...]FSsInodes),
        evictionapi.SignalImageFsAvailable:  rankDiskPressureFunc([...]FSsInodes),
        // ...
    }
    return signalToRankFunc
}
```

**rankMemoryPressure**：按 Pod 内存超出请求量的大小降序排列；若使用量相同，再按 Pod QoS 排（BestEffort > Burstable > Guaranteed）。

**驱逐执行**：每轮最多驱逐一个 Pod，HardEviction 把 `gracePeriodOverride` 设为 0（立即终止）：

```go
// pkg/kubelet/eviction/eviction_manager.go（synchronize 内）
for i := range activePods {
    pod := activePods[i]
    gracePeriodOverride := int64(0)
    if !isHardEvictionThreshold(thresholdToReclaim) {
        gracePeriodOverride = m.config.MaxPodGracePeriodSeconds
    }
    // killPodNow 调用 podWorkers.UpdatePod(SyncPodKill)
    // 最终 syncPod() 把 podPhase 更新为 Failed
    if err := killPod(pod, gracePeriodOverride, ...); err == nil {
        return []*v1.Pod{pod}
    }
}
```

**killPodFunc 对应的是 killPodNow**：

```go
// pkg/kubelet/pod_workers.go:291
func killPodNow(podWorkers PodWorkers, recorder record.EventRecorder) eviction.KillPodFunc {
    // ...
    podWorkers.UpdatePod(UpdatePodOptions{
        Pod:        pod,
        UpdateType: kubetypes.SyncPodKill,
        KillPodOptions: &KillPodOptions{
            CompletedCh:                      ch,
            Evict:                            isEvicted,
            PodStatusFunc:                    statusFn,
            PodTerminationGracePeriodSecondsOverride: gracePeriodOverride,
        },
    })
}
```

---

## §03 容器 QoS 和 OOMScoreAdj 的取值范围

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| OOMScoreAdj 计算 | [qos/policy.go](kubernetes/pkg/kubelet/qos/policy.go) | `GetContainerOOMScoreAdjust:40` |
| 容器启动时设置 | [kuberuntime_container_linux.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container_linux.go) | `:58` |

### 不可压缩资源和节点稳定性

不可压缩资源（内存、磁盘容量和 inode、PID）不够使用时，会严重影响节点稳定性。kubelet 维持节点稳定性有两道防线：

- **第一道防线**：用户进程 kubelet
  - 优先清理停止的 Pod 和不用的镜像
  - 其次根据 Pod 排行打分进行驱逐，根据 Pod 实际使用资源状态、以及用户配置的资源阈值驱逐
- **第二道防线**：Linux OOM Killer
  - 两道防线之间有一种协作：`oom_score_adj`
  - kubelet 在创建容器时设置 `oom_score_adj`，影响 Linux OOM Killer 杀死进程的优先级

### 容器 QoS 分类

Kubernetes 有三种 QoS 类型：

- **Guaranteed**：Pod 中所有容器的 Resource 的 request 都超过限制
- **Burstable**：Pod 中至少有一个 container 没有设置 request 或者 limit
- **BestEffort**：所有 container 都没有设置 request 和 limit

### OOMScoreAdj 取值规则

```go
// pkg/kubelet/qos/policy.go:40
func GetContainerOOMScoreAdjust(pod *v1.Pod, container *v1.Container, memoryCapacity int64) int {
    // 系统核心组件（node critical pod）
    if types.IsNodeCriticalPod(pod) {
        return guaranteedOOMScoreAdj  // -997
    }
    switch v1qos.GetPodQOS(pod) {
    case v1.PodQOSGuaranteed:
        return guaranteedOOMScoreAdj  // -997，最后被 OOM Killer 杀死
    case v1.PodQOSBestEffort:
        return besteffortOOMScoreAdj  // 1000，最先被 OOM Killer 杀死
    }
    // Burstable：按内存请求量比例计算
    oomScoreAdj := 1000 - (1000*memoryRequest)/memoryCapacity
    // 下界：不能小于 1000 + guaranteedOOMScoreAdj = 3
    if int(oomScoreAdj) < (1000 + guaranteedOOMScoreAdj) {
        return 1000 + guaranteedOOMScoreAdj  // = 3
    }
    // 上界：不能等于 besteffortOOMScoreAdj = 1000
    if int(oomScoreAdj) == besteffortOOMScoreAdj {
        return int(oomScoreAdj - 1)          // = 999
    }
    return int(oomScoreAdj)
}
```

**各 QoS 的 OOMScoreAdj 取值范围**：

| Pod 类型 | OOMScoreAdj 取值 |
|---------|----------------|
| Guaranteed（设置了 request/limit，两者相等） | -997 |
| BestEffort（没有设置 request/limit） | 1000 |
| Burstable（request != limit 至少一个设置了） | 3-999 之间 |
| Kubelet 进程 | -999 |
| 至上 node 上的镜像容器 | -999 |

OOMScoreAdj 数值越大，越优先被 OOM Killer 杀死，与 eviction 驱逐优先级方向一致。

### OOMScoreAdj 何时设置

在 `generateLinuxContainerConfig` 中调用，最终在 `startContainer` 中生效：

```go
// pkg/kubelet/kuberuntime/kuberuntime_container_linux.go:58
oomScoreAdj := int64(qos.GetContainerOOMScoreAdjust(pod, container,
    int64(m.machineInfo.MemoryCapacity)))
```

---

## §04 oomWatcher 管理器源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Watcher 接口 | [oom/types.go](kubernetes/pkg/kubelet/oom/types.go) | `Watcher interface:22` |
| realWatcher 结构体 | [oom_watcher_linux.go](kubernetes/pkg/kubelet/oom/oom_watcher_linux.go) | `realWatcher:38` |
| oomWatcher 初始化 | [oom_watcher_linux.go](kubernetes/pkg/kubelet/oom/oom_watcher_linux.go) | `NewWatcher:47` |
| 监控启动 | [oom_watcher_linux.go](kubernetes/pkg/kubelet/oom/oom_watcher_linux.go) | `Start:67` |
| oomparser（内核日志解析） | [kmsgparser.go](kubernetes/vendor/github.com/euank/go-kmsg-parser/kmsgparser/kmsgparser.go) | `NewParser` |

### oomWatcher 作用

oomWatcher 监听 Linux 内核的 OOM 容器信息，底层打开 `/dev/kmsg` 获取内核日志，启动**生产者**分析容器 OOM 类型的日志，启动**消费者**消费并发送 K8s Event。

### Watcher 接口定义

```go
// pkg/kubelet/oom/types.go:22
type Watcher interface {
    Start(ref *v1.ObjectReference) error
}
```

### realWatcher 结构体

```go
// pkg/kubelet/oom/oom_watcher_linux.go:38
type realWatcher struct {
    recorder    record.EventRecorder  // k8s event 记录器
    oomStreamer  streamer              // oom 的信息流
}
```

### NewWatcher 分析

kubelet 在 `kubelet.go:482` 初始化 oomWatcher：

```go
oomWatcher, err := oomwatcher.NewWatcher(kubeDeps.Recorder)
```

`NewWatcher` 底层使用 Cadvisor 的 `oomparser` 作为 OOM 事件生产者：

```go
// pkg/kubelet/oom/oom_watcher_linux.go:47
func NewWatcher(recorder record.EventRecorder) (Watcher, error) {
    // NewWatcher creates and initializes an OOMWatcher backed by Cadvisor as the oom streamer.
    oomStreamer, err := oomparser.New()
    if err != nil {
        return nil, err
    }
    watcher := &realWatcher{
        recorder:    recorder,
        oomStreamer: oomStreamer,
    }
    return watcher, nil
}
```

### oomparser 分析：打开 /dev/kmsg

```go
// vendor/github.com/euank/go-kmsg-parser/kmsgparser/kmsgparser.go
func NewParser() (Parser, error) {
    f, err := os.Open("/dev/kmsg")  // 底层打开 /dev/kmsg
    if err != nil {
        return nil, err
    }
    bootTime, err := getBootTime()
    return &parser{
        log:        &StandardLogger{nil},
        kmsgReader: f,
        bootTime:   bootTime,
    }, nil
}
```

**/dev/kmsg 解读**：内核通过 `printk()` 函数将日志写入一个 ring buffer，有三种方式可读取：`/proc/kmsg`（FIFO 方式，只能一个进程读）、`/dev/kmsg`（可多个进程读，支持 seek）、`dmesg`（读取 ring buffer 内容）。oomWatcher 使用 `/dev/kmsg`。

### 生产者：StreamOoms 分析

```
StreamOoms(outStream chan<- *OomInstance)
  │
  ├─── for msg := range kmsgParser.Parse()
  │      ├─── checkIfContainerName(msg)     — 正则匹配 "invoked oom-killer:"
  │      │      └─── 确认是 OOM 类型日志
  │      └─── containerRegexp.MatchString() — 正则匹配 "oom-kill:constraint=(.*),nodemask=(.*),cpuset=(.*)"
  │             └─── 提取容器进程信息
  └─── outStream <- oomCurrentInstance      — 生产完成，写入 channel
```

- 正则判断是否是 OOM 类型：`regexp.MustCompile("invoked oom-killer:")`
- 正则判断是 OOM 的容器进程：`regexp.MustCompile("oom-kill:constraint=(.*),nodemask=(.*),cpuset=(.*)")`
- 最后构造 `oomCurrentInstance` 写入 `outStream` channel，生产完成

### 消费者分析

```go
// pkg/kubelet/oom/oom_watcher_linux.go:67（Start 函数内）
go func() {
    defer runtime.HandleCrash()
    for event := range outStream {
        if event.VictimContainerName == recordEventContainerName {
            klog.V(1).InfoS("Got sys oom event", "event", event)
            eventMsg := "System OOM encountered"
            if event.ProcessName != "" && event.Pid != 0 {
                eventMsg = fmt.Sprintf("%s, victim process: %s, pid: %d", ...)
            }
            ow.recorder.Eventf(ref, v1.EventTypeWarning, systemOOMEvent, eventMsg)
        }
    }
    klog.ErrorS(nil, "Unexpectedly stopped receiving OOM notifications")
}()
```

消费者和生产者共用一个 `outStream` channel，接收到 OOM 事件后通过 `recorder.Eventf` 发出 K8s Warning Event，用户可通过 `kubectl get events` 看到 OOM 通知。
