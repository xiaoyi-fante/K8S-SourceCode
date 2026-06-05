# 第19章 kubelet 的 syncLoop 其余监听

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 19 章 — kubelet 的 syncLoop 其余监听
> **源码入口**: `pkg/kubelet/kubelet.go` / `pkg/kubelet/kubelet_pods.go`

---

## 核心机制一览

1. **housekeepingCh 是 Pod 清理的定时触发器**：每隔 `housekeepingPeriod` 触发一次，调用 `HandlePodCleanups`，执行终止 pod workers、清理 runtime 残留容器、删除孤儿卷/pod 目录、清理 cgroup、回收 backoff 重试 map 等一系列清理工作。执行时不向 pod workers 发送任何配置更改，因此不会产生新 pod。

2. **HandlePodCleanups 孤儿进程判定标准**：孤儿进程 = 正在运行但不在 podManager 缓存中的进程。`SyncKnownPods` 返回四类状态（`SyncPod/TerminatingPod/TerminatedPod/TerminatedAndRecreatePod`），据此决定各容器是否可能仍在运行（`runningPods/possiblyRunningPods/restartablePods`），再从 runtime 获取实际运行中的 pod 做交叉比对。

3. **syncCh 是 podWorker 的定时 resync 触发器**：每秒触发一次，调用 `getPodsToSync` 从两类来源收集待 sync 的 pod：① `workQueue.GetWork()` 获取到时间点的 `completeWork` 任务；② 遍历 `PodSyncLoopHandlers` 判断是否到期（`activeDeadlineHandler.pastActiveDeadline`）。

4. **completeWork 三级退避策略**：syncPod 成功 → 按 `resyncInterval` 正常间隔入队；网络未就绪 → `backOffOnTransientErrorPeriod` 短暂退避；其他错误 → `workerBackOffPeriod` 较长退避。时间点由 `clock.Now().Add(delay)` 决定，`GetWork` 只返回到时间的任务。

5. **三种探针共用 handleProbeSync 分发**：liveness/readiness/startup 三路 chan 均调用 `handleProbeSync`，内部找到 pod 后统一调用 `handler.HandlePodSyncs([]*v1.Pod{pod})` → `dispatchWork` → podWorker。三者差异在于 handler 上游：liveness 失败直接 kill；readiness 额外调 `SetContainerReadiness`（写 podStatus 缓存并 patch apiserver）；startup 额外调 `SetContainerStartup`。

6. **updateStatusInternal 防止非法状态转移**：`checkContainerStateTransition` 拒绝将 terminated 容器状态回退为 non-terminated，确保状态机单向流转；成功后向 `podStatusChannel` 发请求，由 statusManager 异步 batch 后 patch apiserver。

---

## 全章调用链总图

```
syncLoopIteration (kubelet.go:1926)
  │
  ├─ case <-housekeepingTicker.C:
  │    if !kl.sourcesReady.AllReady() → skip
  │    handler.HandlePodCleanups (kubelet_pods.go:1067)
  │      ├─ pcm.GetAllPodsFromCgroups()        → cgroupPods
  │      ├─ kl.podManager.GetPodsAndMirrorPods() → allPods, mirrorPods
  │      ├─ kl.podWorkers.SyncKnownPods(allPods) → workingPods
  │      │    遍历 podSyncStatuses:
  │      │      !in desiredPods || restartRequested → removeTerminatedWorker
  │      │      else switch status → SyncPod/TerminatingPod/TerminatedPod/TerminatedAndRecreatePod
  │      ├─ 分类 runningPods / possiblyRunningPods / restartablePods
  │      ├─ 清理 probeManager 中的 pod   (prober_manager.go:215: CleanupPods)
  │      ├─ 清理 runtime 中不需要的 pod
  │      │    for runningRuntimePod:
  │      │      state == SyncPod || TerminatingPod → continue
  │      │      else → podWorkers.UpdatePod(SyncPodKill)
  │      ├─ removeOrphanedPodStatuses(allPods, mirrorPods)
  │      ├─ cleanupOrphanedPodDirs(allPods, runningRuntimePods)
  │      │    → removeOrphanedPodVolumeDirs (子卷目录 → syscall.Rmdir)
  │      ├─ deleteOrphanedMirrorPods()
  │      ├─ cleanupOrphanedPodCgroups(cgroupPods, ...)
  │      └─ kl.backOff.GC()   (清理超过 2×maxDuration 未更新的重试记录)
  │
  ├─ case <-syncTicker.C:
  │    podsToSync := kl.getPodsToSync() (kubelet.go:1738)
  │      ├─ workQueue.GetWork()   → 取到时间的 completeWork 任务
  │      └─ PodSyncLoopHandlers.ShouldSync(pod)
  │           → activeDeadlineHandler.pastActiveDeadline()
  │    handler.HandlePodSyncs(podsToSync) (kubelet.go:2180)
  │      → dispatchWork(pod, SyncPodSync, mirrorPod, start)
  │           → podWorkers.UpdatePod(...)
  │                → managePodLoop
  │                     workType: SyncPodWork → syncTerminatingPodFn
  │                               TerminatingPodWork → syncTerminatingPodFn
  │                               TerminatedPodWork → syncTerminatedPodFn
  │
  ├─ case update := <-kl.livenessManager.Updates():
  │    if update.Result == Failure:
  │        handleProbeSync(kl, update, handler, "liveness", "unhealthy")
  │              → handler.HandlePodSyncs([]*v1.Pod{pod})
  │
  ├─ case update := <-kl.readinessManager.Updates():
  │    ready := update.Result == Success
  │    kl.statusManager.SetContainerReadiness(update.PodUID, update.ContainerID, ready)
  │      (status_manager.go:202)  → updateStatusInternal → patch apiserver
  │    handleProbeSync(kl, update, handler, "readiness", status)
  │
  └─ case update := <-kl.startupManager.Updates():
       started := update.Result == Success
       kl.statusManager.SetContainerStartup(update.PodUID, update.ContainerID, started)
         (status_manager.go:263)  → updateStatusInternal → patch apiserver
       handleProbeSync(kl, update, handler, "startup", status)
```

---

## §01 syncLoop 的 housekeepingCh 流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| HandlePodCleanups | [kubelet_pods.go](kubernetes/pkg/kubelet/kubelet_pods.go) | `HandlePodCleanups:1067` |
| 清理 probeManager | [prober/prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `CleanupPods:215` |

### housekeepingCh 的作用与入口

`housekeepingTicker` 在 `syncLoop` 中初始化，每隔 `housekeepingPeriod` 触发：

```go
// syncLoopIteration 中
case <-housekeepingCh:
    if !kl.sourcesReady.AllReady() {
        // sources 未全部就绪时跳过清理，避免误删来自未完成同步的 source 的 pod
        klog.V(4).InfoS("SyncLoop (housekeeping, skipped): sources aren't ready yet")
    } else {
        start := time.Now()
        if err := handler.HandlePodCleanups(); err != nil {
            klog.ErrorS(err, "Failed cleaning pods")
        }
        duration := time.Since(start)
        if duration > housekeepingWarningDuration {
            klog.ErrorS(fmt.Errorf("housekeeping took too long"), "Housekeeping took longer than expected")
        }
    }
```

`AllReady` 判断三个 source（apiserver/file/http）是否全部就绪，是为了防止 kubelet 刚启动尚未同步完 pod 列表时就清理了属于其他 source 的 pod。

### HandlePodCleanups：七项清理工作

`HandlePodCleanups` 注释明确说明：**此函数由主同步循环执行，不应包含任何阻塞调用**（避免阻塞 syncLoop）；执行期间不向 pod workers 发送配置更改。

```go
// pkg/kubelet/kubelet_pods.go:1067
// HandlePodCleanups performs a series of cleanup work, including terminating
// pod workers, killing unwanted pods, and removing orphaned volumes/pod
// directories. No config changes are sent to pod workers while this method
// is executing which means no new pods can appear.
// NOTE: This function is executed by the main sync loop, so it
// should not contain any blocking calls.
func (kl *Kubelet) HandlePodCleanups() error {
```

**第一步：获取 cgroup 信息**

```go
var cgroupPods map[types.UID]cm.CgroupName
if kl.cgroupsPerQOS {
    pcm := kl.containerManager.NewPodContainerManager()
    cgroupPods, err = pcm.GetAllPodsFromCgroups()
}
```

**第二步：获取常规 pod 和 mirror pod**

```go
allPods, mirrorPods := kl.podManager.GetPodsAndMirrorPods()
```

`basicManager` 维护两张 map：`podByUID`（常规 pod）和 `mirrorPodByUID`（静态 pod 的 mirror），以 pod UID 为 key。

**第三步：SyncKnownPods — 获取工作状态 map**

```go
workingPods := kl.podWorkers.SyncKnownPods(allPods)
```

`SyncKnownPods` 的逻辑：
1. 把 `desiredPods`（allPods 的 UID set）和 `podSyncStatuses`（所有 podWorker 的状态追踪器）做对比
2. 若 podUID 不在 `desiredPods` 中，或 status 为 `restartRequested`，则从 podWorker 中清理掉（`removeTerminatedWorker`）
3. 剩余的通过 pod status 判断，填入 `workers` map 返回

四种状态：
```
TerminatedAt.IsZero() && restartRequested → TerminatedAndRecreatePod
TerminatedAt.IsZero() && !restart         → TerminatedPod
TerminatingAt.IsZero()                    → TerminatingPod
default                                    → SyncPod
```

**第四步：分类 runningPods / possiblyRunningPods / restartablePods**

```go
for uid, workerState := range workingPods {
    switch workerState {
    case SyncPod:
        runningPods.Insert(uid)
        possiblyRunningPods.Insert(uid)
    case TerminatingPod:
        possiblyRunningPods.Insert(uid)
    case TerminatedAndRecreatePod:
        restartablePods.Insert(uid)
    }
}
```

**孤儿进程的定义**：正在运行的（来自 runtime）但状态为非 `SyncPod`/`TerminatingPod` 的 pod。

**第五步：清理 probeManager 中的 pod**

```go
kl.probeManager.CleanupPods(desiredPods)  // 传入 runningPods
```

遍历 probeWorker，若 podUID 不在 `desiredPods` 中，则调用 `worker.stop()`，向 `stopCh chan` 发信号。`worker.run` 中的 `probeLoop` 收到 `stopCh` 信号后 break，探针停止运行。

**第六步：清理 runtime 中不需要的 pod**

```go
runningRuntimePods, _ := kl.runtimeCache.GetPods()
for _, runningPod := range runningRuntimePods {
    switch workerState, ok := workingPods[runningPod.ID]; {
    case ok && workerState == SyncPod, ok && workerState == TerminatingPod:
        continue  // pod worker 已在处理，不干预
    default:
        // 非期望状态的 pod：调用 podWorkers.UpdatePod(SyncPodKill) 触发 kill
        podWorkers.UpdatePod(UpdatePodOptions{
            UpdateType:  kubetypes.SyncPodKill,
            KillPodOptions: &KillPodOptions{
                PodTerminationGracePeriodSecondsOverride: &zero,
            },
        })
    }
}
```

**第七步：清理孤儿 pod 状态（podStatus）**

```go
kl.removeOrphanedPodStatuses(allPods, mirrorPods)
```

遍历 `statusManager` 中的 podStatuses，若 pod 不在 `allPods` 和 `mirrorPods` 中则删除——避免 podStatus 缓存无限增长。

**第八步：清理孤儿 pod 目录**

```go
kl.cleanupOrphanedPodDirs(allPods, runningRuntimePods)
```

流程：
1. 合并 `allPods` 和 `runningRuntimePods` 构成 `knownPods` set
2. `listPodDir` 从节点磁盘读取所有 pod 目录
3. 若目录对应的 podUID 不在 `knownPods` 中：
   - 若 pod volumes 未完全卸载 → 跳过（不删目录，避免数据丢失）
   - 否则调用 `removeOrphanedPodVolumeDirs`（清理 subvolumes → `removeall.RemoveAllOneFilesystem` → `syscall.Rmdir`）

**第九步：清理孤儿 MirrorPods**

```go
kl.deleteOrphanedMirrorPods()
```

Mirror pod 通过 pod name（而非 UID）追踪，孤儿 mirror pod 直接删除。

**第十步：清理孤儿 pod cgroup**

```go
kl.cleanupOrphanedPodCgroups(cgroupPods, cgroupPods, possiblyRunningPods)
```

遍历 cgroup 中的 pod，若不在 `possiblyRunningPods` 中且 volume 已卸载，则减少 cgroup 计数并删除 cgroup（`pcm.Destroy(val)`）。

**第十一步：清理 backoff 重试 map**

```go
kl.backOff.GC()
// GC when entry has not been updated for 2*maxDuration
func (p *Backoff) GC() {
    for id, entry := range p.perItemBackoff {
        if now.Sub(entry.lastUpdate) > p.maxDuration*2 {
            delete(p.perItemBackoff, id)
        }
    }
}
```

---

## §02 syncLoop 的 syncCh 流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| syncCh 处理 | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `getPodsToSync:1738` |
| HandlePodSyncs | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `HandlePodSyncs:2180` |
| completeWork 入队 | [kubelet/util/queue/work_queue.go](kubernetes/pkg/kubelet/util/queue/work_queue.go) | `Enqueue:50` |

### syncCh 的作用

syncCh 定时器管道，每隔一秒去同步最新保存的 Pod 状态：

```go
syncTicker := time.NewTicker(time.Second)
defer syncTicker.Stop()
// ...
case <-syncCh:
    podsToSync := kl.getPodsToSync()
    if len(podsToSync) == 0 { break }
    handler.HandlePodSyncs(podsToSync)
```

### getPodsToSync：两类来源

```go
// pkg/kubelet/kubelet.go:1738
func (kl *Kubelet) getPodsToSync() []*v1.Pod {
    allPods := kl.podManager.GetPods()
    podUIDs  := kl.workQueue.GetWork()              // 来源①：workQueue 到时的任务
    podUIDSet := sets.NewString()
    for _, podUID := range podUIDs {
        podUIDSet.Insert(string(podUID))
    }

    var podsToSync []*v1.Pod
    for _, pod := range allPods {
        if podUIDSet.Has(string(pod.UID)) {
            podsToSync = append(podsToSync, pod)
            continue
        }
        // 来源②：PodSyncLoopHandlers 判断
        for _, podSyncLoopHandler := range kl.PodSyncLoopHandlers {
            if podSyncLoopHandler.ShouldSync(pod) {
                podsToSync = append(podsToSync, pod)
                break
            }
        }
    }
    return podsToSync
}
```

**workQueue.GetWork** 返回到期的任务（queue 中 value <= now 的 UID）并删除它们：

```go
func (q *basicWorkQueue) GetWork() []types.UID {
    now := q.clock.Now()
    for k, v := range q.queue {
        if v.Before(now) {
            items = append(items, k)
            delete(q.queue, k)
        }
    }
    return items
}
```

**workQueue.Enqueue** 设置 key 的下次触发时间：

```go
func (q *basicWorkQueue) Enqueue(item types.UID, delay time.Duration) {
    q.queue[item] = q.clock.Now().Add(delay)
}
```

调用 `Enqueue` 的是 `completeWork`，syncPod 完成后根据错误类型决定下次 resync 时间：

```go
func (p *podWorkers) completeWork(pod *v1.Pod, syncErr error) {
    switch {
    case syncErr == nil:
        // 成功：按正常 resync 间隔
        p.workQueue.Enqueue(pod.UID, wait.Jitter(p.resyncInterval, workerResyncIntervalJitterFactor))
    case strings.Contains(syncErr.Error(), NetworkNotReadyErrorMsg):
        // 网络未就绪：短暂退避
        p.workQueue.Enqueue(pod.UID, wait.Jitter(backOffOnTransientErrorPeriod, workerBackOffPeriodJitterFactor))
    default:
        // 其他错误：较长退避
        p.workQueue.Enqueue(pod.UID, wait.Jitter(p.backOffPeriod, workerBackOffPeriodJitterFactor))
    }
    p.completeWorkQueueNext(pod.UID)
}
```

**PodSyncLoopHandlers** 当前只有 `activeDeadlineHandler` 实现了 `ShouldSync`，判断 pod 是否超过了 `ActiveDeadlineSeconds`：

```go
func (m *activeDeadlineHandler) ShouldSync(pod *v1.Pod) bool {
    return m.pastActiveDeadline(pod)
}

func (m *activeDeadlineHandler) pastActiveDeadline(pod *v1.Pod) bool {
    if pod.Spec.ActiveDeadlineSeconds == nil { return false }
    podStatus, ok := m.podStatusProvider.GetPodStatus(pod.UID)
    if !ok { podStatus = pod.Status }
    if podStatus.StartTime.IsZero() { return false }
    start    := podStatus.StartTime.Time
    duration := m.clock.Since(start)
    allowedDuration := time.Duration(*pod.Spec.ActiveDeadlineSeconds) * time.Second
    return duration >= allowedDuration
}
```

### HandlePodSyncs：分发到 podWorker

```go
// pkg/kubelet/kubelet.go:2180
func (kl *Kubelet) HandlePodSyncs(pods []*v1.Pod) {
    start := kl.clock.Now()
    for _, pod := range pods {
        mirrorPod, _ := kl.podManager.GetMirrorPodByPod(pod)
        kl.dispatchWork(pod, kubetypes.SyncPodSync, mirrorPod, start)
    }
}
```

`dispatchWork` 调用 `podWorkers.UpdatePod`，启动 `managePodLoop` goroutine：

```go
go func() {
    defer runtime.HandleCrash()
    p.managePodLoop(podUpdates)
}()
```

`managePodLoop` 中按 `update.WorkType` 分支：
- `SyncPodWork` → 调用 `syncPodFn`（也就是 `kubelet.syncPod`）
- `TerminatingPodWork` → 调用 `syncTerminatingPodFn`
- `TerminatedPodWork` → 调用 `syncTerminatedPodFn`

---

## §03 syncLoop 监听的 health manager

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| handleProbeSync | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `handleProbeSync:2025` |
| SetContainerReadiness | [status/status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `SetContainerReadiness:202` |
| SetContainerStartup | [status/status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `SetContainerStartup:263` |
| updateStatusInternal | [status/status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `updateStatusInternal:391` |

### 三种探针在 syncLoopIteration 中的监听

```go
case update := <-kl.livenessManager.Updates():
    if update.Result == proberesults.Failure {
        handleProbeSync(kl, update, handler, "liveness", "unhealthy")
    }

case update := <-kl.readinessManager.Updates():
    ready := update.Result == proberesults.Success
    kl.statusManager.SetContainerReadiness(update.PodUID, update.ContainerID, ready)
    status := "notReady"
    if ready { status = "ready" }
    handleProbeSync(kl, update, handler, "readiness", status)

case update := <-kl.startupManager.Updates():
    started := update.Result == proberesults.Success
    kl.statusManager.SetContainerStartup(update.PodUID, update.ContainerID, started)
    status := "unhealthy"
    if started { status = "started" }
    handleProbeSync(kl, update, handler, "startup", status)
```

三者共同点：都调用 `handleProbeSync`。差异：
- **liveness 失败**：只触发 pod resync（kubelet 随后在 `computePodActions` 中判断到探针失败，触发容器 kill 并重建）
- **readiness 变化**：先写 podStatus 缓存并 patch apiserver（影响 Service Endpoint 的 ready 状态），再 resync
- **startup 变化**：先写 podStatus 缓存并 patch apiserver，再 resync

### handleProbeSync：探针事件统一分发

```go
// pkg/kubelet/kubelet.go:2025
func handleProbeSync(kl *Kubelet, update proberesults.Update,
    handler SyncHandler, probe, status string) {
    pod, ok := kl.podManager.GetPodByUID(update.PodUID)
    if !ok {
        // pod 已不存在，忽略
        klog.V(4).InfoS("SyncLoop (probe): ignore irrelevant update")
        return
    }
    klog.V(1).InfoS("SyncLoop (probe)", "probe", probe, "status", status, ...)
    handler.HandlePodSyncs([]*v1.Pod{pod})
}
```

`probe` 和 `status` 参数仅用于日志，核心逻辑是把 pod 送给 `HandlePodSyncs` → `dispatchWork` → podWorker。

### SetContainerReadiness：readiness 写入 podStatus 并 patch apiserver

```go
// pkg/kubelet/status/status_manager.go:202
func (m *manager) SetContainerReadiness(podUID types.UID, containerID kubecontainer.ContainerID, ready bool) {
    m.podStatusesLock.Lock()
    defer m.podStatusesLock.Unlock()

    pod, ok := m.podManager.GetPodByUID(podUID)
    if !ok { return }

    oldStatus, found := m.podStatuses[pod.UID]
    if !found { return }

    // 找到对应的 containerStatus
    containerStatus, _, ok := findContainerStatus(&oldStatus.status, containerID.String())
    if !ok { return }

    // 幂等检查
    if containerStatus.Ready == ready { return }

    // 深拷贝，修改 Ready 字段
    status := *oldStatus.status.DeepCopy()
    containerStatus, _, _ = findContainerStatus(&status, containerID.String())
    containerStatus.Ready = ready

    // 更新 ReadyCondition（调 updateConditionFunc）
    // 评估 readiness gate 条件
    // 更新 ContainersReady condition

    m.updateStatusInternal(pod, status, false)
}
```

### updateStatusInternal：防非法转移 + 发 patch 请求

```go
// pkg/kubelet/status/status_manager.go:391
func (m *manager) updateStatusInternal(pod *v1.Pod, status v1.PodStatus, forceUpdate bool) bool {
    // 1. 获取 oldStatus（先查缓存，再查 mirrorPod，再查 pod.Status）
    var oldStatus v1.PodStatus
    cachedStatus, isCached := m.podStatuses[pod.UID]
    if isCached { oldStatus = cachedStatus.status }
    // ...

    // 2. 检验非法状态转移（terminated → non-terminated 不允许）
    if err := checkContainerStateTransition(oldStatus.ContainerStatuses, status.ContainerStatuses, ...); err != nil {
        klog.ErrorS(err, "Status update on pod aborted")
        return false
    }

    // 3. 设置各 condition 的 LastTransitionTime
    updateLastTransitionTime(&status, &oldStatus, v1.ContainersReady)
    updateLastTransitionTime(&status, &oldStatus, v1.PodReady)
    updateLastTransitionTime(&status, &oldStatus, v1.PodInitialized)
    updateLastTransitionTime(&status, &oldStatus, v1.PodScheduled)

    // 4. 向 podStatusChannel 发送请求
    select {
    case m.podStatusChannel <- podStatusSyncRequest{pod.UID, newStatus}:
        return true
    default:
        // channel 满时跳过（由下一次 syncBatch 处理）
        klog.V(4).InfoS("Skipping the status update for pod for now because the channel is full")
        return false
    }
}
```

`podStatusChannel` 在 `statusManager.Start()` 的 goroutine 中监听：

```go
// Start: 监听 podStatusChannel 和 syncTicker
for {
    select {
    case syncRequest := <-m.podStatusChannel:
        // 记录新 status，排空 channel
        for i := len(m.podStatusChannel); i > 0; i-- {
            <-m.podStatusChannel
        }
        m.syncBatch()
    case <-syncTicker.C:
        m.syncBatch()
    }
}
```

`syncBatch` 最终调用 `PatchPodStatus`（`c.CoreV1().Pods(namespace).Patch(..., StrategicMergePatchType, ...)`）将 pod status patch 到 apiserver 更新 etcd，供后续 service controller 使用（readiness 决定 Endpoint 是否接收流量）。
