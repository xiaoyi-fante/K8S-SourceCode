# 第16章 containerRuntime 停止容器的流程

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 16 章 — containerRuntime 停止容器的流程
> **源码入口**: `pkg/kubelet/kuberuntime/kuberuntime_container.go`

---

## 核心机制一览

1. **killContainer 五步流程**：① 获取 containerSpec（pod 为 nil 时从容器标签恢复）→ ② 计算 gracePeriod → ③ 执行 internalLifecycle.PreStopContainer（当前为空） → ④ 执行用户配置的 Lifecycle.PreStop hook → ⑤ gRPC 调用 runtime.StopContainer。

2. **containerSpec 从标签恢复的原因**：删除 pod 时 kubelet 重启可能丢失 pod 对象；为防止这种情况，创建容器时已将关键 pod 信息写入容器标签，kill 时通过 `restoreSpecsFromContainerLabels` → `runtimeService.ContainerStatus` → 解析标签恢复 containerSpec。

3. **gracePeriod 三层覆盖规则**：基础值取 `DeletionGracePeriodSeconds`；若容器处于 `TerminationReasonStartupProbe`/`LivenessProbe` 失败则缩为 2；若处于 `TerminationReasonStartupProbe` 失败则进一步缩；最终若 `gracePeriodOverride != nil` 则直接用 override 值。执行完 PreStop hook 后，若剩余 gracePeriod < 2s，保底给 2s，避免不必要的 SIGKILL。

4. **PreStop hook 与 PostStart hook 共用 runner.Run**：执行类型优先级相同（Exec > HTTPGet > TCPSocket 不支持），用 `select + timer` 监听 done channel 实现超时控制。

5. **killContainer 的六类调用方**：PostStart hook 失败后删除（调用方 01）、SyncPod Step 2 sandbox 变化时（调用方 02）、容器需要重启且处于 unknown 状态时（调用方 03）、liveness/readiness/startup 探针失败或容器配置变化时（调用方 04）、kubelet.killPod（调用方 05）、containerGC 垃圾回收（调用方 06）。

6. **containerGC 的 removeOldestN 策略**：从最新到最旧排序，删除超过上限的旧容器；若容器处于 unknown 状态（可能仍在运行）则先尝试 kill 再 remove，防止激进清理留下孤儿容器。

---

## 全章调用链总图

```
killContainer (kuberuntime_container.go:599)
  │
  ├─ Step 1: 获取 containerSpec
  │    if pod != nil:
  │      GetContainerSpec(pod, containerName)   (container/helpers.go:288)
  │    else:
  │      restoreSpecsFromContainerLabels(containerID)  (kuberuntime_container.go:560)
  │        └─ runtimeService.ContainerStatus(containerID)
  │             └─ 解析 Labels/Annotations → 重建 pod + container 对象
  │
  ├─ Step 2: 计算 gracePeriod
  │    基础值: pod.DeletionGracePeriodSeconds
  │    缩减条件: TerminationReason (StartupProbe/LivenessProbe/ReadinessProbe)
  │    override: gracePeriodOverride != nil → 直接替换
  │
  ├─ Step 3: internalLifecycle.PreStopContainer(containerID)
  │    当前实现: return nil（空钩子，预留扩展）
  │
  ├─ Step 4: Lifecycle.PreStop hook（用户配置）
  │    if containerSpec.Lifecycle.PreStop != nil && gracePeriod > 0:
  │      runner.Run(containerID, pod, containerSpec, handler)
  │        ├─ Exec   → RunInContainer → ExecSync gRPC
  │        └─ HTTPGet → runHTTPHandler
  │      select {
  │        case <-done:   // hook 完成
  │        case <-timer:  // gracePeriod 超时
  │      }
  │      执行后: if gracePeriod < 2 { gracePeriod = 2 }（保底）
  │             if gracePeriodOverride != nil { gracePeriod = *gracePeriodOverride }
  │
  └─ Step 5: StopContainer gRPC (remote_runtime.go:262)
       m.runtimeService.StopContainer(containerID, gracePeriod)
       失败 → event: FailedToStopContainer

killContainer 的六类调用方：
  调用方 01  PostStart hook 失败 → 立即 kill 容器
  调用方 02  SyncPod Step 2: sandbox 变化 → killPodWithSyncResult → killContainersWithSyncResult
  调用方 03  computePodActions: 容器 unknown → 加入 ContainersToKill
  调用方 04  computePodActions: 探针失败/配置变化 → 加入 ContainersToKill
  调用方 05  kubelet.killPod (kubelet_pods.go:847) → KillPod → killPodWithSyncResult
  调用方 06  containerGC.removeOldestN (kuberuntime_gc.go:125)
```

---

## §01 killContainer 源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| killContainer 主流程 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `killContainer:599` |
| containerSpec 从标签恢复 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `restoreSpecsFromContainerLabels:560` |
| GetContainerSpec | [container/helpers.go](kubernetes/pkg/kubelet/container/helpers.go) | `GetContainerSpec:288` |
| StopContainer gRPC | [cri/remote/remote_runtime.go](kubernetes/pkg/kubelet/cri/remote/remote_runtime.go) | `StopContainer:262` |

### Step 1：获取 containerSpec

`killContainer` 的签名：

```go
// pkg/kubelet/kuberuntime/kuberuntime_container.go:599
func (m *kubeGenericRuntimeManager) killContainer(
    pod *v1.Pod,
    containerID kubecontainer.ContainerID,
    containerName string,
    message string,
    reason containerKillReason,
    gracePeriodOverride *int64) error {
```

入参说明：
- `pod`：被 kill 容器所属的 pod 信息（可能为 nil）
- `containerID`：目标容器的运行时 ID
- `containerName`：容器名，用于在 pod.Spec 中找到对应 containerSpec
- `message`：kill 原因描述，写入 event
- `reason`：结构化 kill 原因（影响 gracePeriod 计算）
- `gracePeriodOverride`：调用方强制指定的宽限期（nil 则使用 pod 默认值）

**当 pod != nil 时**，直接遍历 `pod.Spec` 找到与 `containerName` 匹配的 containerSpec：

```go
// pkg/kubelet/container/helpers.go:288
func GetContainerSpec(pod *v1.Pod, containerName string) *v1.Container {
    var containerSpec *v1.Container
    podutil.VisitContainers(&pod.Spec, podutil.AllFeatureEnabledContainers(),
        func(c *v1.Container) bool {
            if containerName == c.Name {
                containerSpec = c
                return false
            }
            return true
        })
    return containerSpec
}
```

**当 pod == nil 时**（kubelet 重启、pod 对象已消失），调用 `restoreSpecsFromContainerLabels` 从容器标签中恢复：

```go
// pkg/kubelet/kuberuntime/kuberuntime_container.go:560
func (m *kubeGenericRuntimeManager) restoreSpecsFromContainerLabels(
    containerID kubecontainer.ContainerID) (*v1.Pod, *v1.Container, error) {

    var pod *v1.Pod
    var container *v1.Container
    s, err := m.runtimeService.ContainerStatus(containerID.ID)  // gRPC 获取容器状态
    // ...
    l := getContainerInfoFromLabels(s.Labels)
    a := getContainerInfoFromAnnotations(s.Annotations)
    // 从 Labels/Annotations 重建最小化 pod 和 container 对象
    pod = &v1.Pod{
        ObjectMeta: metav1.ObjectMeta{
            UID:       l.PodUID,
            Name:      l.PodName,
            Namespace: l.PodNamespace,
            DeletionGracePeriodSeconds: a.PodDeletionGracePeriod,
        },
        Spec: v1.PodSpec{
            TerminationGracePeriodSeconds: a.PodTerminationGracePeriod,
        },
    }
    container = &v1.Container{
        Name:                   l.ContainerName,
        Ports:                  a.ContainerPorts,
        TerminationMessagePath: a.TerminationMessagePath,
    }
    if a.PreStopHandler != nil {
        container.Lifecycle = &v1.Lifecycle{PreStop: a.PreStopHandler}
    }
    return pod, container, nil
}
```

标签恢复的关键设计：创建容器时已将 pod 的关键字段（UID、Name、Namespace、GracePeriod、PreStop hook）写入容器的 Labels/Annotations，使得即使 pod 对象消失，kill 流程也能正确执行 PreStop 并计算 gracePeriod。

### Step 2：计算 gracePeriod

gracePeriod 初始值来自 `pod.DeletionGracePeriodSeconds`（若为 0 则取 `minimumGracePeriodInSeconds`）：

```go
// 基础值
gracePeriod := int64(minimumGracePeriodInSeconds)  // 2s 下限

switch {
case pod.DeletionGracePeriodSeconds != nil:
    gracePeriod = *pod.DeletionGracePeriodSeconds
case pod.Spec.TerminationGracePeriodSeconds != nil:
    gracePeriod = *pod.Spec.TerminationGracePeriodSeconds
}
```

根据 kill reason 进一步缩减：
- `TerminationReasonStartupProbe`：缩为 `pod.Spec.TerminationGracePeriodSeconds / 2`（最小 2s）
- `TerminationReasonLivenessProbe`：同上
- 其他 reason：保持原值

若调用方传入了 `gracePeriodOverride`，最终直接替换（调用方优先）：

```go
if gracePeriodOverride != nil {
    gracePeriod = *gracePeriodOverride
}
```

### Step 3：PreStopContainer 内部钩子

```go
// Run internal pre-stop lifecycle hook
if err := m.internalLifecycle.PreStopContainer(containerID.ID); err != nil {
    return err
}
```

当前实现为空（`return nil`），是预留给资源管理器的扩展点。对比 `PreStartContainer` 在启动时注册 containerID 进各 manager 映射，`PreStopContainer` 在 v1.21 中尚未有对应的注销逻辑（注销在 `PostStopContainer` 完成）。

### Step 4：用户配置的 Lifecycle.PreStop hook

```go
// pkg/kubelet/kuberuntime/kuberuntime_container.go
if containerSpec.Lifecycle != nil && containerSpec.Lifecycle.PreStop != nil && gracePeriod > 0 {
    // runner.Run 执行 hook（Exec 或 HTTPGet）
    // 用 done channel + timer 实现超时控制
    done := make(chan struct{})
    go func() {
        defer close(done)
        if err := m.runner.Run(containerID, pod, containerSpec,
            containerSpec.Lifecycle.PreStop); err != nil {
            // 记录 "PreStop hook failed" event
        }
    }()
    select {
    case <-time.After(time.Duration(gracePeriod) * time.Second):
        // hook 未在 gracePeriod 内完成，记录 "PreStop hook completed not in grace period" log
    case <-done:
        // hook 正常完成
    }
}
```

执行完 PreStop 后，kubelet 保证容器至少还有 2s 宽限期（防止 SIGKILL 来得太快），同时再次应用 `gracePeriodOverride`：

```go
// always give containers a minimal shutdown window to avoid unnecessary SIGKILLs
if gracePeriod < minimumGracePeriodInSeconds {
    gracePeriod = minimumGracePeriodInSeconds  // 2s
}
if gracePeriodOverride != nil {
    gracePeriod = *gracePeriodOverride
}
```

### Step 5：gRPC 调用 runtime.StopContainer

```go
// pkg/kubelet/cri/remote/remote_runtime.go:262
func (r *remoteRuntimeService) StopContainer(containerID string, timeout int64) error {
    // timeout 即 gracePeriod（秒），runtime 用此值决定 SIGTERM→SIGKILL 的等待时长
    // 超时 = default(2min) + gracePeriod，给 runtime 足够时间
    t := r.timeout + time.Duration(timeout)*time.Second
    // ...
    _, err = r.runtimeClient.StopContainer(ctx, &runtimeapi.StopContainerRequest{
        ContainerId: containerID,
        Timeout:     timeout,
    })
    return err
}
```

StopContainer 失败时记录 `FailedToStopContainer` event，否则容器进入 Stopped 状态等待 GC。

---

## §02 killContainer 的调用方解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| SyncPod sandbox 变化路径 | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `killContainersWithSyncResult:677` |
| computePodActions unknown/探针失败 | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `computePodActions:516` |
| kubelet.killPod | [kubelet_pods.go](kubernetes/pkg/kubelet/kubelet_pods.go) | `killPod:847` |
| KillPod / killPodWithSyncResult | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `KillPod:938` / `killPodWithSyncResult:945` |
| containerGC removeOldestN | [kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `removeOldestN:125` |

### 调用方 01：PostStart hook 失败后删除

PostStart hook 执行失败时，`startContainer` 立即 kill 该容器（reason = `reasonUnknown`，message 为空），触发容器重建流程：

```go
// startContainer 内 Step 4 失败分支
if handlerErr != nil {
    m.killContainer(pod, kubecontainer.ContainerID{...}, container.Name,
        "FailedPostStartHook", reasonUnknown, nil)
    // 记录 FailedPostStartHook event
}
```

### 调用方 02：SyncPod Step 2 sandbox 变化

当 `podContainerChanges.KillPod == true`（sandbox 需要重建）时，SyncPod 调用 `killPodWithSyncResult`，再调用 `killContainersWithSyncResult`，并行 kill pod 内所有容器：

```go
// pkg/kubelet/kuberuntime/kuberuntime_manager.go:677
func (m *kubeGenericRuntimeManager) killContainersWithSyncResult(
    pod *v1.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64,
) (syncResults []*kubecontainer.SyncResult) {
    containerResults := make(chan *kubecontainer.SyncResult, len(runningPod.Containers))
    wg := sync.WaitGroup{}
    for _, container := range runningPod.Containers {
        go func(container *kubecontainer.Container) {
            defer wg.Done()
            killResult := m.NewSyncResult(kubecontainer.KillContainer, container.Name)
            if err := m.killContainer(pod, container.ID, container.Name,
                "Need to kill Pod", reasonUnknown, gracePeriodOverride); err != nil {
                killResult.Fail(err, ...)
            }
            containerResults <- killResult
        }(container)
    }
    // ...
}
```

这是 killContainer 唯一的**并行**调用场景——sandbox 重建时同时 kill 所有容器加速清理。

### 调用方 03：容器处于 unknown 状态需要重启

`computePodActions` 在检查 init 容器时，若容器状态为 unknown（既非 running 也非 exited），将其加入 `ContainersToKill`，同时加入 `ContainersToStart` 触发重建：

```go
// pkg/kubelet/kuberuntime/kuberuntime_manager.go:computePodActions
if containerStatus == nil || !containerStatus.State.IsRunning() {
    // unknown 状态：不确定是否还在运行，先 kill 再重启
    changes.ContainersToKill[containerStatus.ID] = containerToKillInfo{
        name:      containerStatus.Name,
        container: &pod.Spec.Containers[idx],
        message:   fmt.Sprintf("Init container is in %q state", containerStatus.State),
        reason:    reasonUnknown,
    }
    changes.ContainersToStart = append(changes.ContainersToStart, idx)
}
```

unknown 状态发生原因：运行时（containerd/docker）异常导致容器状态查询失败，kubelet 保守地认为"可能仍在运行"，先 kill 再重启，避免留下孤儿容器。

### 调用方 04：探针失败或容器配置变化

`computePodActions` 在检查非 init 容器时，通过 `shouldRestartContainer` 判断是否需要重启，三类情况加入 `ContainersToKill`：

```go
// 三类 kill 场景（按优先级）
if restart, changed := shouldRestartContainer(pod, container, containerStatus); restart {
    if changed {
        // 容器配置变化（image/command 等）→ restart + kill
        message = fmt.Sprintf("Container definition changed")
        reason = reasonUnknown
    } else if liveness, found = lm.GetLivenessProbe(pod, container); found &&
        liveness.Fail(containerStatus.ID) {
        // liveness 探针失败 → kill，reason 影响 gracePeriod 缩减
        reason = reasonLivenessProbe
    } else if startup, found = sm.GetStartupProbe(pod, container); found &&
        startup.Fail(containerStatus.ID) {
        reason = reasonStartupProbe
    }
    // keepCount < 0: 不想重启但需要 kill（如 pod 终止中）
    changes.ContainersToKill[containerStatus.ID] = containerToKillInfo{...}
    if restart {
        changes.ContainersToStart = append(changes.ContainersToStart, idx)
    }
}
```

### 调用方 05：通过 kubelet.killPod 进入

kubelet 对外暴露 `killPod`（供 podWorker 调用）：

```go
// pkg/kubelet/kubelet_pods.go:847
func (kl *Kubelet) killPod(pod *v1.Pod, runningPod *kubecontainer.Pod,
    status *kubecontainer.PodStatus, gracePeriodOverride *int64) error {
    kl.containerRuntime.KillPod(pod, p, gracePeriodOverride)
    kl.containerManager.UpdateQOSCgroups()  // 更新 QoS cgroup
}
```

`KillPod`（kuberuntime_manager.go:938）直接委托给 `killPodWithSyncResult`，后者并行 kill 所有容器后返回聚合结果。

podWorker 通过 `syncTerminatingPod` 触发此路径，`syncTerminatingPod` 会根据 pod.UpdateType 决定是调用 `KillPodOptions` 还是直接 `syncPod` 做最后一次对账：

```go
// podWorker 内部
switch pod.UpdateType {
case TerminatePod:
    err = p.syncTerminatingPod(ctx, pod, status, podStatus, gracePeriod, ...)
case TerminatedPod:
    err = p.syncTerminatedPod(ctx, pod, status)
}
```

### 调用方 06：containerGC 垃圾回收

containerGC 定期清理已退出容器，核心函数 `removeOldestN`（kuberuntime_gc.go:125）：

```go
// pkg/kubelet/kuberuntime/kuberuntime_gc.go:125
func (cgc *containerGC) removeOldestN(containers []containerGCInfo, toRemove int) []containerGCInfo {
    // containers 已按创建时间从新到旧排序，删除末尾最旧的 toRemove 个
    numToKeep := len(containers) - toRemove
    sort.Sort(byCreated(containers))
    for i := len(containers) - 1; i >= numToKeep; i-- {
        id := containers[i].id
        // 若容器处于 unknown 状态（可能仍在运行），先 kill
        if containers[i].unknown {
            m := fmt.Sprintf("Container is in unknown state, try killing it before removal")
            if err := cgc.manager.killContainer(nil, id, containers[i].name, m, ...); err != nil {
                klog.Errorf("Failed to stop container %q", id)
                continue  // kill 失败则跳过，避免 remove 孤儿进程
            }
        }
        if err := cgc.manager.removeContainer(containers[i].id); err != nil {
            klog.Errorf("Failed to remove container %q", id)
        }
    }
    return containers[:numToKeep]
}
```

GC 只清理**已退出**的容器；处于 unknown 状态的容器在 remove 前先 kill，防止删除元数据后进程继续运行成为孤儿进程。
