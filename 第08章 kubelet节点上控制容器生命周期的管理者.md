# 第08章 kubelet 节点上控制容器生命周期的管理者

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 08 章 — kubelet 节点上控制容器生命周期的管理者
> **源码入口**: `cmd/kubelet/app/server.go`

---

## 核心机制一览

1. **cobra 启动链**：`main()` → cobra Run → `Run()` → `run()` → `RunKubelet()` → `startKubelet()`，最终调用 `kubelet.Run()` 进入 `syncLoop` 永久循环。`NewMainKubelet` 负责构建所有子组件（volumeManager、podManager、secretManager 等），在 `startKubelet` 之前完成。

2. **节点自注册（registerWithAPIServer）**：kubelet 启动时默认开启 `registerNode`，调用 `registerWithAPIServer` 向 apiserver 创建 Node 对象。失败会按指数退避重试（200ms 起，最长 7 秒）。首次注册前先通过一系列工厂函数填充 `node.Status`（CPU/Memory/VersionInfo/Conditions 等字段），然后调用 `kubeClient.CoreV1().Nodes().Create()`。若 node 已存在则取回 apiserver 中的版本做 patch 对比，判断是否需要更新。

3. **双心跳机制（NodeStatus + Lease）**：kubelet 每 10 秒执行 `syncNodeStatus`，满足"信息有变化 **且** 距上次上报超过 5 分钟"才 patch 一次 NodeStatus 到 apiserver——这是重量级心跳。Lease 心跳则独立且更轻量，每 10 秒（`NodeLeaseDurationSeconds/4`）更新 `kube-node-lease` 命名空间中本节点的 Lease 对象；两套心跳相互独立，Lease 失败同样采用指数退避。

4. **syncLoop 主循环**：`syncLoopIteration` 以 `select` 监听 6 个 channel：`configCh`（来自 apiserver 的 Pod 增删改）、`plegCh`（容器运行时事件）、`syncCh`（定时同步）、`livenessManager/readinessManager/startupManager`（健康检查结果）、`housekeepingCh`（清理）。Pod 新增走 `HandlePodAdditions` → `dispatchWork` → `podWorkers.UpdatePod`（异步）→ `managePodLoop` → `syncPod`。

5. **syncPod 六步执行**：生成 pod status → 更新 statusManager → 准备数据目录 → 挂载 volume（VolumeManager）→ 拉取 secrets → 调用 `containerRuntime.SyncPod` 创建容器。整个流程串行在 pod worker goroutine 中执行。

6. **podManager 内存管理器**：`basicManager` 维护节点上所有 Pod（含 mirrorPod）的内存索引（`podByUID`、`podByFullName`、`mirrorPodByFullName`、`translationByUID`）。任何 Pod 的增删改都同步更新 podManager；`secretManager` 和 `configMapManager` 也随之注册/注销。mirrorPod 是 standalone（static pod）在集群中的镜像，两者通过 UID 映射关联。

7. **volumeManager 三子模块**：`volumePluginMgr`（通过 informer 同步 CSI driver 信息）、`desiredStateOfWorldPopulator`（轮询 podManager，把 pod 的 volume 需求写入 desiredStateOfWorld）、`reconciler`（对比 desiredStateOfWorld 与 actualStateOfWorld，执行 attach/detach/mount/unmount 操作）。三者在 `volumeManager.Run` 中异步启动。

8. **reconciler 三步顺序**：`reconcile()` 固定执行 unmountVolumes → mountAttachVolumes → unmountDetachDevices，先卸载不再需要的 volume 再挂载新的，防止同一 volume 被两个 pod 同时持有。kubelet 重启后的 `sync()` 通过扫描 `/var/lib/kubelet/pods` 目录重建 actualStateOfWorld，避免重复 attach 已挂载的 volume。

9. **statusManager 异步双路设计**：`SetPodStatus` 把状态写入容量 1000 的 `podStatusChannel`（写满时跳过），主循环以 select 同时监听 channel（实时）和 10 秒定时器（兜底 `syncBatch`）。真正写 apiserver 在 `syncPod` 中：先 `needsUpdate` 判断，再 GET 当前版本，最后 `PatchPodStatus`。`needsReconcile` 只比对 kubelet 管辖的四个 Condition，避免外部控制器的修改触发无效 patch。

10. **probeManager worker-per-probe 模式**：`AddPod` 为每个容器的每种探针（liveness/readiness/startup）各创建一个独立 goroutine worker，key 为 `{podUID, containerName, probeType}`。`doProbe` 实现"初始延迟 → 连续阈值 → 写 resultsManager → updates channel → syncLoopIteration"的完整闭环；results 的消费者正是 kubelet 主循环，liveness 失败触发重启，readiness 变化更新 Service 后端。

---

## 全章调用链总图

```
main()                                          // cmd/kubelet/main.go
  │
  ▼ cobra.Command.Execute()
  │
  ▼ Run()                                       // server.go:434
  │
  ▼ run()                                       // server.go:492
  │   ├─ NewMainKubelet()                       // kubelet.go:344  构建所有子组件
  │   └─ RunKubelet()                           // server.go:1091
  │         └─ startKubelet()                   // server.go:1195
  │               └─ kl.Run(podCfg.Updates())
  │
  ▼ kubelet.Run()                               // kubelet.go
  │
  ├─ go kl.volumeManager.Run()                 // 异步：volumeManager 三子模块
  │       ├─ go volumePluginMgr.Run()          //   CSI driver informer 同步
  │       ├─ go desiredStateOfWorldPopulator.Run()  // 轮询 podManager → desiredStateOfWorld
  │       └─ go reconciler.Run()               //   对比两张状态表 → attach/mount/unmount
  │             └─ reconcile()                 //   unmountVolumes → mountAttachVolumes → unmountDetachDevices
  │             └─ sync()                      //   首次扫 /var/lib/kubelet/pods 重建 actualStateOfWorld
  │
  ├─ go kl.nodeLeaseController.Run()           // 异步：Lease 轻量心跳（每 10 秒）
  │
  ├─ go kl.syncNodeStatus()                    // 异步：NodeStatus 重量心跳（变化+5分钟门槛）
  │
  ├─ go kl.statusManager.Start()              // 异步：pod 状态同步到 apiserver
  │       ├─ case <-podStatusChannel          //   实时（容量 1000，非阻塞写入）
  │       └─ case <-syncTicker (10s)          //   兜底 syncBatch（遍历 podStatuses 内存缓存）
  │             └─ syncPod() → PatchPodStatus //   needsUpdate/needsReconcile 过滤后 patch
  │
  ├─ kl.probeManager（NewMainKubelet 时初始化，HandlePodAdditions 时 AddPod 启动 worker）
  │       └─ per-container per-probeType goroutine（go w.run()）
  │             └─ doProbe()                  //   InitialDelaySeconds → probe() → 阈值 → resultsManager.Set
  │                   └─ updates chan         //   → syncLoopIteration 消费
  │
  └─ kl.syncLoop()                            // kubelet.go:1852
  │
  ▼ syncLoop() → syncLoopIteration()          // kubelet.go:1926  六路 select
  │   ├─ case <-configCh (ADD)
  │   │     └─ HandlePodAdditions()           // kubelet.go:2084
  │   │           ├─ podManager.AddPod()
  │   │           ├─ probeManager.AddPod()    // 为新 pod 启动探针 worker
  │   │           └─ dispatchWork()           // kubelet.go:2039
  │   │                 └─ podWorkers.UpdatePod()  ← 异步
  │   │                       └─ managePodLoop() → syncPod()
  │   ├─ case <-plegCh                        // PLEG 容器运行时事件
  │   ├─ case <-syncCh                        // 定时强制同步
  │   ├─ case <-livenessManager.Updates()     // liveness 失败 → 重启容器
  │   ├─ case <-readinessManager.Updates()    // readiness 变化 → SetContainerReadiness
  │   ├─ case <-startupManager.Updates()      // startup 完成 → 解锁 liveness/readiness
  │   └─ case <-housekeepingCh               // 清理终止的 pod
  │
  ▼ syncPod()                                 // kubelet.go:1497
      ├─ 1. generateAPIPodStatus()
      ├─ 2. statusManager.SetPodStatus()
      ├─ 3. os.MkdirAll(pod data dirs)
      ├─ 4. volumeManager.WaitForAttachAndMount()
      ├─ 5. pullSecrets = kl.getPullSecretsForPod()
      └─ 6. containerRuntime.SyncPod()
```

---

## §01 kubelet 启动主流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| cobra 入口 | [server.go](kubernetes/cmd/kubelet/app/server.go) | `Run:434` |
| run 主函数 | [server.go](kubernetes/cmd/kubelet/app/server.go) | `run:492` |
| 构建 kubelet 实例 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `NewMainKubelet:344` |
| 启动 kubelet | [server.go](kubernetes/cmd/kubelet/app/server.go) | `RunKubelet:1091` |
| 进入主循环 | [server.go](kubernetes/cmd/kubelet/app/server.go) | `startKubelet:1195` |
| 主循环 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `syncLoop:1852` |
| 主循环迭代 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `syncLoopIteration:1926` |

```
main()                               // cmd/kubelet/main.go
  │
  ▼ cobra.Command.Execute()
  │
  ▼ Run()                            // server.go:434
  │
  ▼ run()                            // server.go:492
  │   ├─ NewMainKubelet()            // kubelet.go:344  实例化所有子组件
  │   └─ RunKubelet()               // server.go:1091
  │         └─ startKubelet()       // server.go:1195
  │               └─ go kl.Run()   // 进入主循环，永不退出
  │
  ▼ kl.Run() → kl.syncLoop()        // kubelet.go:1852
  │
  ▼ syncLoopIteration()             // kubelet.go:1926  六路 select 事件调度
```

本节覆盖从 `main()` 到 `syncLoopIteration` 的完整启动链。

### 启动链概览

kubelet 的启动链结构与 kube-scheduler、kube-controller-manager 一致：cobra 根命令 → `Run` → `run` → `RunKubelet` → `startKubelet`，最终调用 `kl.Run()` 进入永久运行状态。

`NewMainKubelet`（`kubelet.go:344`）是 kubelet 的构建函数，在进入主循环前完成所有子组件的实例化：

```go
// pkg/kubelet/kubelet.go:344（节选）
func NewMainKubelet(kubeCfg *kubeletconfiginternal.KubeletConfiguration, ...) (*Kubelet, error) {
    klet := &Kubelet{}
    // 构建 podManager、volumeManager、secretManager、configMapManager
    // 构建 statusManager、probeManager、evictionManager
    // 注册 nodeLeaseController（lease 心跳）
    ...
    return klet, nil
}
```

`startKubelet` 接到 kubelet 实例后启动主 goroutine：

```go
// cmd/kubelet/app/server.go:1195（节选）
func startKubelet(k kubelet.Bootstrap, ...) {
    go k.Run(podCfg.Updates())  // 进入 syncLoop，永不退出
    if enableServer {
        go k.ListenAndServe(...)
    }
}
```

### syncLoopIteration — 六路 select

`syncLoopIteration` 是 kubelet 事件调度的核心，通过 `select` 监听 6 个 channel：

```go
// pkg/kubelet/kubelet.go:1926（节选）
func (kl *Kubelet) syncLoopIteration(configCh <-chan kubetypes.PodUpdate,
    handler SyncHandler, syncCh <-chan time.Time,
    housekeepingCh <-chan time.Time, plegCh <-chan *pleg.PodLifecycleEvent) bool {
    select {
    case u, open := <-configCh:     // 来自 apiserver：ADD/UPDATE/REMOVE/RECONCILE
        ...
        handler.HandlePodAdditions(u.Pods)
    case e := <-plegCh:            // PLEG 容器运行时事件（容器启停）
    case <-syncCh:                 // 定时强制同步
    case update := <-kl.livenessManager.Updates():   // liveness 健康检查
    case update := <-kl.readinessManager.Updates():  // readiness 健康检查
    case update := <-kl.startupManager.Updates():    // startup 健康检查
    case <-housekeepingCh:         // 清理终止的 pod
    }
    return true
}
```

`configCh` 是重点路径：它来自 apiserver 的 watch，携带 Pod 的增删改事件，驱动 `HandlePodAdditions` 进入 pod 创建流程。

---

## §02 kubelet 节点自注册源码分析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 自注册入口 | [kubelet_node_status.go](kubernetes/pkg/kubelet/kubelet_node_status.go) | `registerWithAPIServer:52` |
| 单次注册尝试 | [kubelet_node_status.go](kubernetes/pkg/kubelet/kubelet_node_status.go) | `tryRegisterWithAPIServer:86` |

### 自注册流程

kubelet 默认开启 `registerNode=true`，在 `Run()` 里调用 `registerWithAPIServer`，按指数退避不断重试直到成功：

```go
// pkg/kubelet/kubelet_node_status.go:52
func (kl *Kubelet) registerWithAPIServer() {
    // 指数退避：200ms → 400ms → ... 最长 7 秒
    backoff := wait.Backoff{...}
    wait.ExponentialBackoff(backoff, func() (bool, error) {
        node, err := kl.initialNode(context.TODO())
        // 填充 node.Status 所有字段
        return kl.tryRegisterWithAPIServer(node), nil
    })
}
```

注册前的 `initialNode()` 调用一系列工厂函数填充 `node.Status`：

| Status 字段 | 来源 |
|------------|------|
| `Capacity.cpu/memory` | `cadvisor.MachineInfo`（本机硬件信息） |
| `Capacity.pods` | `kl.maxPods`（kubelet 配置） |
| `VersionInfo` | 容器运行时 `GetCAdvisorContainerInfo` |
| `Conditions` | `MemoryPressure/DiskPressure/PIDPressure/Ready` 工厂函数 |
| `DaemonEndpoints` | kubelet 监听的 ip+port |
| `Images` | 节点镜像列表 |
| `VolumeLimits` | 各 volume 插件的 `GetCSIDevicePluginCapacity` |

`tryRegisterWithAPIServer` 调用 `kubeClient.CoreV1().Nodes().Create()` 创建节点：

```go
// pkg/kubelet/kubelet_node_status.go:86（节选）
func (kl *Kubelet) tryRegisterWithAPIServer(node *v1.Node) bool {
    _, err := kl.kubeClient.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
    if err == nil {
        return true
    }
    if !apierrors.IsAlreadyExists(err) {
        // 不是"已存在"错误（如网络故障）→ 返回 false 触发重试
        return false
    }
    // node 已存在：取回 apiserver 版本，对比后决定是否 patch
    existingNode, err := kl.kubeClient.CoreV1().Nodes().Get(context.TODO(), string(kl.nodeName), ...)
    ...
    // 若内容不同，patch 更新
    _, _, err = nodeutil.PatchNodeStatus(kl.kubeClient.CoreV1(), types.NodeName(kl.nodeName), ...)
    return err == nil
}
```

设计要点：注册失败分两种情况处理——网络/apiserver 故障直接重试；node 已存在则取回对比，避免重启后覆盖已有状态。

---

## §03 基于 NodeStatus 和 Lease 对象的心跳机制

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| NodeStatus 心跳入口 | [kubelet_node_status.go](kubernetes/pkg/kubelet/kubelet_node_status.go) | `syncNodeStatus:445` |
| 更新 NodeStatus | [kubelet_node_status.go](kubernetes/pkg/kubelet/kubelet_node_status.go) | `updateNodeStatus:463` |
| 实际上报逻辑 | [kubelet_node_status.go](kubernetes/pkg/kubelet/kubelet_node_status.go) | `tryUpdateNodeStatus:480` |
| Lease 控制器初始化 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `NewMainKubelet:344` |
| Lease 控制器 Run | [controller.go](kubernetes/staging/src/k8s.io/component-helpers/apimachinery/lease/controller.go) | `Run:93` |
| Lease sync | [controller.go](kubernetes/staging/src/k8s.io/component-helpers/apimachinery/lease/controller.go) | `sync:101` |

### 两种心跳对比

| | NodeStatus 心跳 | Lease 心跳 |
|--|----------------|-----------|
| 上报内容 | 节点完整 Status（条件、容量、地址等） | 只更新 Lease 的 `renewTime` 字段 |
| 触发条件 | 信息有变化 **且** 距上次上报 ≥ 5 分钟 | 每 10 秒无条件更新 |
| 存储位置 | Node 对象本身 | `kube-node-lease` 命名空间中的 Lease 对象 |
| 性能开销 | 重量级（携带大量数据） | 轻量级（适合大规模集群） |
| 失败重试 | 最多 5 次（`nodeStatusUpdateRetry`），失败调用 `onRepeatedHeartbeatFailure` 关闭连接 | 指数退避，200ms 起最长 7 秒 |

### NodeStatus 心跳

`syncNodeStatus` 每 10 秒被调用一次（由 `wait.Until` 驱动），但实际上报有双重门槛：

```go
// pkg/kubelet/kubelet_node_status.go:480（节选）
func (kl *Kubelet) tryUpdateNodeStatus(tryNumber int) error {
    // 1. 从 apiserver 获取当前 node
    node, err := kl.heartbeatClient.CoreV1().Nodes().Get(...)
    // 2. 用容器运行时 podCIDR 信息判断是否有变化
    podCIDRChanged := false
    if len(node.Spec.PodCIDRs) != 0 {
        podCIDRs := strings.Join(node.Spec.PodCIDRs, ",")
        if podCIDRChanged, err = kl.updatePodCIDR(podCIDRs); err != nil {
            klog.ErrorS(err, "Error updating pod CIDR")
        }
    }
    // 3. 调用工厂函数更新 status 各字段
    kl.setNodeStatus(node)
    // 4. 双重门槛：信息有变化 OR 距上次上报超过 5 分钟
    now := kl.clock.Now()
    if now.Before(kl.lastStatusReportTime.Add(kl.nodeStatusReportFrequency)) {
        if !podCIDRChanged && !nodeStatusHasChanged(&originalNode.Status, &node.Status) {
            return nil  // 不上报
        }
    }
    // 5. patch 上报
    updatedNode, _, err := nodeutil.PatchNodeStatus(kl.heartbeatClient.CoreV1(), ...)
    ...
}
```

### Lease 心跳

Lease 在 `NewMainKubelet` 中初始化：

```go
// pkg/kubelet/kubelet.go:344（节选）
klet.nodeLeaseController = lease.NewController(
    klet.clock,
    klet.heartbeatClient,
    string(klet.nodeName),
    kubeCfg.NodeLeaseDurationSeconds,   // 默认 40 秒
    klet.onRepeatedHeartbeatFailure,
    renewInterval,                       // NodeLeaseDurationSeconds/4 = 10 秒
    v1.NamespaceNodeLease,
    util.SetNodeOwnerFunc(klet.heartbeatClient, string(klet.nodeName)))
```

`lease.controller.Run` 以 `wait.JitterUntil` 每 10 秒调用 `sync`，`sync` 乐观地假设 Lease 未被其他组件修改（因为只有 kubelet 自己更新它），直接 update；若失败才退回到 get-then-update。

---

## §04 syncLoop 响应 pod 创建的过程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 主循环迭代 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `syncLoopIteration:1926` |
| Pod 新增回调 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `HandlePodAdditions:2084` |
| 分发 pod 工作 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `dispatchWork:2039` |
| pod 同步主函数 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `syncPod:1497` |

### HandlePodAdditions

`configCh` 上收到 `kubetypes.ADD` 事件后进入 `HandlePodAdditions`：

```go
// pkg/kubelet/kubelet.go:2084（节选）
func (kl *Kubelet) HandlePodAdditions(pods []*v1.Pod) {
    for _, pod := range pods {
        // 更新 podManager 内存索引
        existingPods := kl.podManager.GetPods()
        kl.podManager.AddPod(pod)

        // mirrorPod 过滤：来自 configCh 中的 mirrorPod 不在本地处理
        if kubetypes.IsMirrorPod(pod) {
            kl.handleMirrorPod(pod, start)
            continue
        }

        // 准入控制：检查节点是否能接纳这个 pod
        if !kl.podWorkers.IsPodTerminationRequested(pod.UID) {
            activePods := kl.filterOutTerminatedPods(existingPods)
            if ok, reason, message := kl.canAdmitPod(activePods, pod); !ok {
                kl.rejectPod(pod, reason, message)
                continue
            }
        }
        // 分发给 pod worker 异步处理
        kl.dispatchWork(pod, kubetypes.SyncPodCreate, mirrorPod, start)
    }
}
```

### dispatchWork → podWorkers.UpdatePod → syncPod

`dispatchWork` 调用 `podWorkers.UpdatePod`，后者为每个 pod 维护一个专属 goroutine（`managePodLoop`），保证同一个 pod 的操作串行执行：

```go
// pkg/kubelet/kubelet.go:2039（节选）
func (kl *Kubelet) dispatchWork(pod *v1.Pod, syncType kubetypes.SyncPodType,
    mirrorPod *v1.Pod, start time.Time) {
    kl.podWorkers.UpdatePod(UpdatePodOptions{
        Pod:        pod,
        MirrorPod:  mirrorPod,
        UpdateType: syncType,
        StartTime:  start,
    })
    // 同时记录 metrics：kubelet_pod_worker_start_duration_seconds
}
```

`managePodLoop` 收到请求后调用 `kl.syncPod`，这是 pod 生命周期管理的核心：

```go
// pkg/kubelet/kubelet.go:1497（节选）
func (kl *Kubelet) syncPod(o syncPodOptions) error {
    pod := o.pod
    // 1. 生成最终的 pod status
    apiPodStatus := kl.generateAPIPodStatus(pod, podStatus)
    // 2. 更新 statusManager（状态异步同步给 apiserver）
    kl.statusManager.SetPodStatus(pod, apiPodStatus)
    // 3. 准备 pod 数据目录
    if err := kl.makePodDataDirs(pod); err != nil { return err }
    // 4. 等待 volume 挂载完成
    if !kl.podWorkers.IsPodTerminationRequested(pod.UID) {
        if err := kl.volumeManager.WaitForAttachAndMount(pod); err != nil {
            return err
        }
    }
    // 5. 拉取 secrets
    pullSecrets := kl.getPullSecretsForPod(pod)
    // 6. 调用容器运行时创建容器
    result := kl.containerRuntime.SyncPod(pod, podStatus, pullSecrets, kl.backOff)
    return result.Error()
}
```

六步执行串行：每步失败都会中断并返回错误，pod worker 会将 pod 重新入队等待下次同步。

---

## §05 kubelet 维护 pod 的内存管理器 podManager 源码解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口 | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `Manager interface:45` |
| basicManager 结构 | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `basicManager:100` |
| AddPod | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `AddPod:148` |
| UpdatePod | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `UpdatePod:152` |
| updatePodsInternal | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `updatePodsInternal:165` |
| DeletePod | [pod_manager.go](kubernetes/pkg/kubelet/pod/pod_manager.go) | `DeletePod:214` |

### podManager 的职责

podManager 是 kubelet 的内存数据库，存储本节点上所有 Pod 的信息。所有对 pod 的增删改操作都必须同步更新 podManager，其他组件（syncPod、volumeManager、probeManager 等）从 podManager 读取 pod 信息而不是每次去 apiserver 查询。

### 什么是 mirrorPod

mirrorPod 与 kubelet 的 standalone 模式有关。如果 pod 通过文件或 HTTP 的形式获得，这个 pod 被称为 **static pod**，k8s 会在集群中创建一个对应的 mirrorPod 来表示这个 static pod。系统很难管理这部分 pod，所以在 kubelet 中创建一个 static pod 对应的 mirrorPod，表示 static pod 在 API 层面的投影。

### basicManager 数据结构

```go
// pkg/kubelet/pod/pod_manager.go:100
type basicManager struct {
    lock sync.RWMutex

    // 核心索引：4 张 map
    podByUID        map[kubetypes.ResolvedPodUID]*v1.Pod  // UID → Pod
    mirrorPodByUID  map[kubetypes.MirrorPodUID]*v1.Pod   // mirror UID → mirror Pod
    podByFullName   map[string]*v1.Pod                   // "namespace/name" → Pod
    mirrorPodByFullName map[string]*v1.Pod               // "namespace/name" → mirror Pod
    translationByUID map[kubetypes.MirrorPodUID]kubetypes.ResolvedPodUID // mirror UID → regular UID

    // 关联组件
    secretManager    secret.Manager
    configMapManager configmap.Manager
    MirrorClient     mirrorpod.Client
}
```

### AddPod / updatePodsInternal

`AddPod` 底层调用 `UpdatePod`，两者最终都走 `updatePodsInternal`：

```go
// pkg/kubelet/pod/pod_manager.go:165（节选）
func (pm *basicManager) updatePodsInternal(pods ...*v1.Pod) {
    for _, pod := range pods {
        // 判断是否是 terminated 状态（PodFailed 或 PodSucceeded）
        if pm.secretManager != nil {
            if isPodInTerminatedState(pod) {
                pm.secretManager.UnregisterPod(pod)  // 注销 secret 引用
            } else {
                pm.secretManager.RegisterPod(pod)    // 注册 secret 引用
            }
        }
        // configMapManager 同理
        ...
        // 判断是否为 mirrorPod
        if kubetypes.IsMirrorPod(pod) {
            // 更新 mirrorPodByUID、mirrorPodByFullName
            pm.mirrorPodByFullName[podFullName] = pod
            ...
        } else {
            // 更新正常 pod 的索引
            resolvedPodUID := kubetypes.ResolvedPodUID(pod.UID)
            pm.podByUID[resolvedPodUID] = pod
            pm.podByFullName[podFullName] = pod
            if mirror, ok := pm.mirrorPodByFullName[podFullName]; ok {
                pm.translationByUID[kubetypes.MirrorPodUID(mirror.UID)] = resolvedPodUID
            }
        }
    }
}
```

### secretManager — 引用计数式缓存

`secretManager` 通过 `cacheBasedManager` 维护 pod 引用的所有 secret。`RegisterPod` 时：

1. 调用 `getReferencedObjects`（即 `getSecretNames`）获取 pod 引用的所有 secret 名称
2. 遍历 names，在 `objectStore` 中 `AddReference`
3. 以 `namespace/name` 为 key，更新 `registeredPods` map

`UnregisterPod` 时从 `registeredPods` 和 `objectStore` 中删除，避免缓存泄漏。

底层的 `objectCache` 通过 `listSecret`/`watchSecret` 实现 watch-based 缓存，结构为 `map[objectKey]*objectCacheItem`，每个 item 持有一个 watch 连接。

### DeletePod

`DeletePod` 先注销 `secretManager` 和 `configMapManager` 中的引用，再从四张 map 中删除对应条目：

```go
// pkg/kubelet/pod/pod_manager.go:214（节选）
func (pm *basicManager) DeletePod(pod *v1.Pod) {
    key := objectKey{namespace: pod.Namespace, name: pod.Name}
    ...
    if pm.secretManager != nil { pm.secretManager.UnregisterPod(pod) }
    if pm.configMapManager != nil { pm.configMapManager.UnregisterPod(pod) }

    podFullName := kubecontainer.GetPodFullName(pod)
    // 删除 map 中所有相关条目（正常 pod 和 mirrorPod 两条路径）
    delete(pm.podByUID, ...)
    delete(pm.podByFullName, podFullName)
    ...
}
```

---

## §06 volumeManager 中的 desiredStateOfWorld 理想状态解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| VolumeManager 接口 | [volume_manager.go](kubernetes/pkg/kubelet/volumemanager/volume_manager.go) | `VolumeManager interface:98` |
| VolumeManager 结构 | [volume_manager.go](kubernetes/pkg/kubelet/volumemanager/volume_manager.go) | `volumeManager struct:217` |
| 初始化 | [volume_manager.go](kubernetes/pkg/kubelet/volumemanager/volume_manager.go) | `NewVolumeManager:151` |
| 启动三子模块 | [volume_manager.go](kubernetes/pkg/kubelet/volumemanager/volume_manager.go) | `Run:260` |
| Populator 初始化 | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `NewDesiredStateOfWorldPopulator:82` |
| Populator Run | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `Run:139` |
| 填充循环 | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `populatorLoop:164` |
| 添加新 pod 的 volume | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `findAndAddNewPods:190` |
| 删除已消失 pod 的 volume | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `findAndRemoveDeletedPods:216` |
| 处理单个 pod 的 volume | [desired_state_of_world_populator.go](kubernetes/pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go) | `processPodVolumes:307` |

### volumeManager 架构

```
Kubelet
  │
  └─ volumeManager.Run()                           // volume_manager.go:260
        │
        ├─ go vm.volumePluginMgr.Run(stopCh)       // 通过 informer 同步 CSI driver 信息
        │
        ├─ go vm.desiredStateOfWorldPopulator.Run() // 轮询 podManager，填充 desiredStateOfWorld
        │         └─ populatorLoop()
        │               ├─ findAndAddNewPods()      // pod 新增或 volume 变化
        │               └─ findAndRemoveDeletedPods() // pod 消失后清理
        │
        └─ go vm.reconciler.Run(stopCh)             // 对比期望与实际，执行 attach/mount
```

三个子模块在 `volumeManager.Run` 中以独立 goroutine 异步启动。

### volumeManager 两张状态表

| 状态表 | 含义 | 谁写入 |
|--------|------|--------|
| `desiredStateOfWorld` | 预期状态：哪些 pod 需要哪些 volume 被 attach | desiredStateOfWorldPopulator |
| `actualStateOfWorld` | 实际状态：哪个 volume 已被 attach 到哪个 node、哪个 pod mount 了 | reconciler（执行完操作后更新） |

reconciler 的工作就是消除这两张表的差距。

### volumePluginMgr 初始化

在 `NewVolumeManager` 时初始化 `volumePluginMgr`，它收集所有内置 volume 插件（emptyDir、hostPath、secret、configMap、NFS、iSCSI、CephFS、GCE PD 等）组成列表：

```go
// cmd/kubelet/app/plugins.go（节选）
func ProbeVolumePlugins(volumeConfig ...) []volume.VolumePlugin {
    allPlugins := []volume.VolumePlugin{}
    allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
    allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(...)...)
    allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
    allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
    // ... nfs, iscsi, cephfs, gcepd, azure, local, ...
    return allPlugins
}
```

CSI driver 信息通过 informer 动态同步，不在静态列表中。

### desiredStateOfWorldPopulator — populatorLoop

Populator 的 `Run` 等待 sources（apiserver 等）就绪后进入 `populatorLoop`：

```go
// pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:164
func (dswp *desiredStateOfWorldPopulator) populatorLoop() {
    dswp.findAndAddNewPods()
    // 频率控制：findAndRemoveDeletedPods 比 findAndAddNewPods 调用频率低
    if time.Since(dswp.timeOfLastGetPodStatus) >= dswp.getPodStatusRetryDuration {
        dswp.findAndRemoveDeletedPods()
    }
}
```

### findAndAddNewPods — 填充 desiredStateOfWorld

从 podManager 取出所有 pod，对每个非 terminated 的 pod 调用 `processPodVolumes`：

```go
// pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:190（节选）
func (dswp *desiredStateOfWorldPopulator) findAndAddNewPods() {
    // 先从 actualStateOfWorld 取出已挂载的 volume（用于防重复）
    mountedVolumesForPod := make(map[volumetypes.UniquePodName]map[string]cache.MountedVolume)
    for _, mountedVolume := range dswp.actualStateOfWorld.GetMountedVolumes() {
        mountedVolumesForPod[mountedVolume.PodName][mountedVolume.OuterVolumeSpecName] = mountedVolume
    }
    // 遍历所有 pod
    for _, pod := range dswp.podManager.GetPods() {
        if dswp.podStateProvider.ShouldPodContainersBeTerminating(pod.UID) {
            continue  // 跳过 terminating 的 pod
        }
        dswp.processPodVolumes(pod, mountedVolumesForPod, ...)
    }
}
```

### processPodVolumes — 单 pod volume 处理

读取 pod 的 `Spec.Volumes`，为每个 volume 创建 `VolumeSpec`，然后调用 `dswp.desiredStateOfWorld.AddPodToVolume` 将 pod-volume 对写入期望状态表：

```go
// pkg/kubelet/volumemanager/populator/desired_state_of_world_populator.go:307（节选）
func (dswp *desiredStateOfWorldPopulator) processPodVolumes(pod *v1.Pod, ...) {
    mounts, devices := util.GetPodVolumeNames(pod)  // 获取 mount 类型
    for _, podVolume := range pod.Spec.Volumes {
        if !mounts.Has(podVolume.Name) && !devices.Has(podVolume.Name) {
            klog.V(4).InfoS("Skipping unused volume", ...)
            continue
        }
        pvc, volumeSpec, volumeGidValue, err := dswp.createVolumeSpec(podVolume, pod, ...)
        ...
        // 写入期望状态表
        dswp.desiredStateOfWorld.AddPodToVolume(
            uniquePodName, volumeName, volumeSpec, ...)
    }
}
```

### findAndRemoveDeletedPods — 清理消失 pod 的 volume

对 pod manager 中已不存在的 pod（即 pod 被删除），将其对应的 volume 从 desiredStateOfWorld 中移除。移除前还要确认 containerRuntime 中该 pod 的所有容器确实已终止，避免误删仍在运行中 pod 的 volume 记录。

---

## §07 volumeManager 中的 reconciler 协调器解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| reconciler Run | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `Run:145` |
| reconciliation 主循环 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `reconciliationLoopFunc:149` |
| reconcile 三步 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `reconcile:163` |
| 卸载 volume | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `unmountVolumes:180` |
| 挂载/attach volume | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `mountAttachVolumes:202` |
| 卸载 block 设备 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `unmountDetachDevices:294` |
| sync（重建实际状态） | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `sync:346` |
| syncStates | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `syncStates:385` |

### reconciler.Run → reconcile 三步

```go
// pkg/kubelet/volumemanager/reconciler/reconciler.go:145
func (rc *reconciler) Run(stopCh <-chan struct{}) {
    wait.Until(rc.reconciliationLoopFunc(), rc.loopSleepDuration, stopCh)
}

// reconciler.go:149
func (rc *reconciler) reconciliationLoopFunc() func() {
    return func() {
        rc.reconcile()
        // reconcile 之后检查：若 populator 已完成首次填充但 sync 尚未跑过，触发一次 sync
        // sync 扫磁盘重建 actualStateOfWorld，之后 StatesHasBeenSynced() 永远返回 true
        if rc.populatorHasAddedPods() && !rc.StatesHasBeenSynced() {
            klog.InfoS("Reconciler: start to sync state")
            rc.sync()
        }
    }
}

// reconciler.go:163（节选）
func (rc *reconciler) reconcile() {
    // 1. 先卸载不再需要的 volume（防止 pod 重建时新 pod mount 前旧 pod 还占用同一卷）
    rc.unmountVolumes()
    // 2. mount 需要的 volume（kubelet 负责 attach，若底层 PVC resize 也在此处理）
    rc.mountAttachVolumes()
    // 3. detach/unmount 不再使用的 block 设备
    rc.unmountDetachDevices()
}
```

三步顺序固定：先 unmount 再 mount，确保同一个 volume 不会被两个 pod 同时持有。

### unmountVolumes — 卸载不再需要的 volume

遍历 actualStateOfWorld 中已挂载的 volume，检查是否还在 desiredStateOfWorld 中：

```go
// reconciler.go:180（节选）
func (rc *reconciler) unmountVolumes() {
    for _, mountedVolume := range rc.actualStateOfWorld.GetAllMountedVolumes() {
        if !rc.desiredStateOfWorld.PodExistsInVolume(mountedVolume.PodName, mountedVolume.VolumeName) {
            // pod 不再需要这个 volume，执行 unmount
            err = rc.operationExecutor.UnmountVolume(mountedVolume, rc.actualStateOfWorld, podsDir)
        }
    }
}
```

`operationExecutor.UnmountVolume` 根据 volume 类型（Filesystem 还是 Block）走不同路径：
- **Filesystem**：`GenerateUnmountVolumeFunc` → 找插件 → `NewUnmounter` → 清理子目录 → `TearDown`
- **Block**：`GenerateUnmapVolumeFunc` → 释放文件描述符锁 → 解绑 pod-device 链接 → 解绑 node-device 链接

### mountAttachVolumes — 挂载需要的 volume

遍历 desiredStateOfWorld，对还没有 attach/mount 的 volume 执行操作：

```go
// reconciler.go:202（节选）
func (rc *reconciler) mountAttachVolumes() {
    for _, volumeToMount := range rc.desiredStateOfWorld.GetVolumesToMount() {
        volMounted, devicePath, err := rc.actualStateOfWorld.PodExistsInVolume(...)
        if cache.IsVolumeNotAttachedError(err) {
            // 未 attach，触发 attach（kubelet 负责时）
            rc.operationExecutor.AttachVolume(volumeToMount, rc.actualStateOfWorld)
        } else if !volMounted || cache.IsRemountRequiredError(err) {
            // 未 mount 或需要 remount（PVC resize 场景）
            rc.operationExecutor.MountVolume(...)
        }
    }
}
```

### sync — 重建 actualStateOfWorld

kubelet 重启后 actualStateOfWorld 是空的，但节点磁盘上可能已有挂载记录。`sync` 通过扫描 `/var/lib/kubelet/pods` 目录重建实际状态：

```go
// reconciler.go:346（节选）
func (rc *reconciler) sync() {
    defer rc.updateLastSyncTime()
    rc.syncStates()
}
```

`syncStates` 扫描磁盘上每个 pod 的卷目录，比对 desiredStateOfWorld：

- **volume 在 DSW 中存在**：标记为 InUse，交给 `reconcile` re-mount
- **volume 不在 DSW 中**：调用 `reconstructVolume` 尝试重建，失败则 `cleanupMounts` 清理残留挂载点

这一机制保证 kubelet 重启后不会因为 actualStateOfWorld 为空而重复 attach 已经挂载的 volume。

---

## §08 statusManager 同步 pod 状态

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `Manager interface:90` |
| 初始化 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `NewManager:119` |
| 启动主循环 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `Start:148` |
| 外部调用入口 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `SetPodStatus:189` |
| 内部更新逻辑 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `updateStatusInternal:391` |
| 批量同步 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `syncBatch:502` |
| 同步单个 pod | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `syncPod:549` |
| 判断是否需要更新 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `needsUpdate:621` |
| 判断是否可以删除 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `canBeDeleted:633` |
| 协调判断 | [status_manager.go](kubernetes/pkg/kubelet/status/status_manager.go) | `needsReconcile:649` |

### statusManager 的职责

statusManager **不主动监控** pod 的状态，而是提供接口供其他 manager 调用（`SetPodStatus`、`SetContainerReadiness` 等），内部异步 patch 到 apiserver。它相当于把 podManager 中负责同步 podStatus 的部分单独抽出来，数据结构上仍依赖 podManager。

### Manager 接口

```go
// pkg/kubelet/status/status_manager.go（节选）
type Manager interface {
    PodStatusProvider
    Start()
    // syncPod 中调用，设置 pod status，版本+1 后发往 podStatusChannel
    SetPodStatus(pod *v1.Pod, status v1.PodStatus)
    SetContainerReadiness(podUID types.UID, containerID kubecontainer.ContainerID, ready bool)
    SetContainerStartup(podUID types.UID, containerID kubecontainer.ContainerID, started bool)
    TerminatePod(pod *v1.Pod)
    RemoveOrphanedStatuses(podUIDs map[types.UID]bool)
}
```

### manager 结构体

```go
// pkg/kubelet/status/status_manager.go（节选）
type manager struct {
    kubeClient   clientset.Interface
    podManager   kubepod.Manager
    // 内存缓存：UID → versionedPodStatus（含版本号）
    podStatuses  map[types.UID]versionedPodStatus
    podStatusesLock sync.RWMutex
    // 异步队列：容量 1000，写满时 SetPodStatus 走 default 分支跳过，等 syncBatch
    podStatusChannel chan podStatusSyncRequest
    apiStatusVersions map[kubetypes.MirrorPodUID]uint64
    podDeletionSafety PodDeletionSafetyProvider
}
```

### Start — 双路 select 主循环

`Start` 以 `go wait.Forever` 运行，监听两个事件：

```go
// status_manager.go:148（节选）
func (m *manager) Start() {
    syncTicker := time.Tick(syncPeriod)  // 默认 10 秒
    go wait.Forever(func() {
        for {
            select {
            case syncRequest := <-m.podStatusChannel:
                // 单个 pod status 变化 → 立即 syncPod
                m.syncPod(syncRequest.podUID, syncRequest.status)
            case <-syncTicker:
                // 定时批量同步：遍历内存缓存 podStatuses，捞起 channel 满时被跳过的更新
                m.syncBatch()
            }
        }
    }, 0)
}
```

两条路径互补：`podStatusChannel` 保证实时性，`syncBatch` 兜底处理 channel 满时被跳过的更新。

### SetPodStatus → updateStatusInternal

`syncPod` 中调用 `SetPodStatus`，底层走 `updateStatusInternal`：

```go
// status_manager.go:391（节选）
func (m *manager) updateStatusInternal(pod *v1.Pod, status v1.PodStatus, forceUpdate bool) bool {
    // 1. 从缓存取旧 status（先查 podStatuses，再查 mirrorPod，最后用 pod.Status）
    var oldStatus v1.PodStatus
    cachedStatus, isCached := m.podStatuses[pod.UID]
    if isCached { oldStatus = cachedStatus.status } else { oldStatus = pod.Status }

    // 2. 校验：容器状态不能从 terminated → 非 terminated（非法状态转换）
    if err := checkContainerStateTransition(oldStatus.ContainerStatuses, status.ContainerStatuses); err != nil {
        return false
    }
    // 3. 更新四个 Condition 的 LastTransitionTime
    updateLastTransitionTime(&status, &oldStatus, v1.ContainersReady)
    updateLastTransitionTime(&status, &oldStatus, v1.PodReady)
    ...
    // 4. 构建 versionedPodStatus，版本号+1，写入 podStatusChannel（非阻塞）
    newStatus := versionedPodStatus{status: status, version: cachedStatus.version + 1, ...}
    m.podStatuses[pod.UID] = newStatus
    select {
    case m.podStatusChannel <- podStatusSyncRequest{pod.UID, newStatus}:
        return true
    default:
        // channel 满，跳过；等 syncBatch 下次捞起
        return false
    }
}
```

### syncPod — 实际 patch 到 apiserver

`syncPod` 是最终写 apiserver 的地方：

1. `needsUpdate` 判断是否需要更新（apiStatusVersions 版本号落后 / podManager 中不存在 / `canBeDeleted` → 都返回 true）
2. 从 apiserver GET 当前 pod 实例
3. 翻译 UID（mirror pod → static pod）
4. 校验 pod 是否刚被重建（UID 不匹配则跳过）
5. 调用 `statusutil.PatchPodStatus` patch 状态
6. 若 status 显示 pod 已 terminated 且 `canBeDeleted` 为 true，调用 `kubeClient.CoreV1().Pods().Delete`

`canBeDeleted` 的判断标准（`PodResourcesAreReclaimed`）：pod terminated **且** 所有容器已停止 **且** volume 已清理 **且** cgroup sandbox 已清理。

### needsUpdate vs needsReconcile

| 函数 | 触发时机 | 判断逻辑 |
|------|----------|---------|
| `needsUpdate` | `syncBatch` 批量遍历时 | apiStatusVersions 版本号落后 / podManager 中不存在 / `canBeDeleted` |
| `needsReconcile` | `syncBatch` 内对 updatedStatuses 二次过滤 | 检查 `pod.Status.Conditions` 内容是否与缓存一致（用 `isPodStatusByKubeletEqual`） |

`isPodStatusByKubeletEqual` 只比对 kubelet 管辖的 Condition（`ContainersReady/PodReady/Initialized/PodScheduled`），忽略外部控制器写入的其他 Condition，避免因外部修改触发不必要的 patch。

---

## §09 probeManager 监控 pod 中容器的健康状况

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Manager 接口 | [prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `Manager interface:56` |
| Manager 结构体 | [prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `manager struct:74` |
| 初始化 | [prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `NewManager:99` |
| 新增 pod 探针 | [prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `AddPod:153` |
| 删除 pod 探针 | [prober_manager.go](kubernetes/pkg/kubelet/prober/prober_manager.go) | `RemovePod:199` |
| worker 初始化 | [worker.go](kubernetes/pkg/kubelet/prober/worker.go) | `newWorker:80` |
| worker 主循环 | [worker.go](kubernetes/pkg/kubelet/prober/worker.go) | `run:131` |
| 单次探测逻辑 | [worker.go](kubernetes/pkg/kubelet/prober/worker.go) | `doProbe:180` |
| prober 初始化 | [prober.go](kubernetes/pkg/kubelet/prober/prober.go) | `newProber:64` |
| exec 探针实现 | [exec.go](kubernetes/pkg/probe/exec/exec.go) | `Probe:50` |
| http 探针实现 | [http.go](kubernetes/pkg/probe/http/http.go) | `Probe:72` |
| tcp 探针实现 | [tcp.go](kubernetes/pkg/probe/tcp/tcp.go) | `Probe:42` |

### 三种探针

| 探针类型 | 作用 | 失败处理 |
|---------|------|---------|
| **liveness** | 判断容器是否存活；捕捉死锁等不能自恢复的场景 | kubelet 重启容器 |
| **readiness** | 判断容器是否可以接收流量；Pod 未就绪时从 Service 负载均衡中摘除 | 不重启，只修改 Ready condition |
| **startupProbe** | 慢启动容器保护；配置后 liveness/readiness 在 startup 成功前不生效 | startup 失败则 kill 容器，避免启动期误杀 |

探针的探测方法有三种：

| 探测方法 | 成功条件 | 源文件 |
|---------|---------|--------|
| **exec** | 命令退出码为 0 | `pkg/probe/exec/exec.go:Probe:50` |
| **http** | HTTP 响应状态码在 200–399 之间 | `pkg/probe/http/http.go:Probe:72` |
| **tcp** | 能成功建立 TCP 连接 | `pkg/probe/tcp/tcp.go:Probe:42` |

### proberManager 结构

```go
// pkg/kubelet/prober/prober_manager.go（节选）
type manager struct {
    // 活跃 worker map，key = probeKey{podUID, containerName, probeType}
    workers   map[probeKey]*worker
    workerLock sync.RWMutex

    statusManager    status.Manager      // 提供 pod id 和 ip
    readinessManager results.Manager     // readiness 探活结果缓存
    livenessManager  results.Manager     // liveness 探活结果缓存
    startupManager   results.Manager     // startup 探活结果缓存
    prober           *prober             // 执行者（含 exec/http/tcp 三种实现）
    start            time.Time
}
```

三个 `results.Manager`（`readinessManager`/`livenessManager`/`startupManager`）底层结构相同：`cache map[ContainerID]Result` + `updates chan Update`。`Result` 类型是 int，值为 `Unknown(-1)`、`Success(0)`、`Failure(1)`。

### 初始化

在 `NewMainKubelet` 中初始化：

```go
// kubelet.go（节选）
klet.probeManager = prober.NewManager(
    klet.statusManager,
    klet.livenessManager,
    klet.readinessManager,
    klet.startupManager,
    klet.runner,
    kubeDeps.Recorder)
```

`newProber` 构建三种底层探针实现：

```go
// prober.go:64（节选）
func newProber(runner kubecontainer.CommandRunner, recorder record.EventRecorder) *prober {
    return &prober{
        exec:          execprobe.New(),
        readinessHTTP: httpprobe.New(followNonLocalRedirects),
        livenessHTTP:  httpprobe.New(followNonLocalRedirects),
        startupHTTP:   httpprobe.New(followNonLocalRedirects),
        tcp:           tcpprobe.New(),
        runner:        runner,
        recorder:      recorder,
    }
}
```

### AddPod — 为每个容器的每种探针创建 worker

kubelet 在 `HandlePodAdditions` 中调用 `kl.probeManager.AddPod(pod)`：

```go
// prober_manager.go:153（节选）
func (m *manager) AddPod(pod *v1.Pod) {
    m.workerLock.Lock()
    defer m.workerLock.Unlock()
    key := probeKey{podUID: pod.UID}
    for _, c := range pod.Spec.Containers {
        key.containerName = c.Name
        // 为三种探针各创建一个 worker（若配置了的话）
        if c.StartupProbe != nil {
            key.probeType = startup
            if _, ok := m.workers[key]; ok { continue }  // 已存在则跳过
            w := newWorker(m, startup, pod, c)
            m.workers[key] = w
            go w.run()  // 独立 goroutine 持续探测
        }
        if c.ReadinessProbe != nil { ... }
        if c.LivenessProbe != nil { ... }
    }
}
```

每个 worker 是一个独立 goroutine，key 为 `{podUID, containerName, probeType}`，保证每个容器每种探针只有一个 worker 在运行。

### worker.doProbe — 单次探测核心逻辑

`doProbe` 返回 `keepGoing bool`，false 代表终止 worker：

```go
// worker.go:180（节选）
func (w *worker) doProbe() (keepGoing bool) {
    // 1. pod 不存在或已删除 → 继续保持（等待 GC）
    status, ok := w.probeManager.statusManager.GetPodStatus(w.pod.UID)
    if !ok { return true }

    // 2. pod 已 terminated → 终止 worker
    if status.Phase == v1.PodFailed || status.Phase == v1.PodSucceeded {
        return false
    }

    // 3. 容器不存在 → 继续等待
    c, ok := podutil.GetContainerStatus(status.ContainerStatuses, w.container.Name)
    if !ok || len(c.ContainerID) == 0 { return true }

    // 4. 容器 ID 变了（重启）→ 更新 containerID，重置 onHold
    if w.containerID.String() != c.ContainerID {
        w.containerID = kubecontainer.ParseContainerID(c.ContainerID)
        w.resultsManager.Remove(w.containerID)
        w.onHold = false
    }
    if w.onHold { return true }  // 容器失败且不重启，等待 GC

    // 5. 容器未运行（启动时间不足 InitialDelaySeconds）→ 跳过
    if int32(time.Since(c.State.Running.StartedAt.Time).Seconds()) < w.spec.InitialDelaySeconds {
        return true
    }

    // 6. 调用 prober.probe 执行实际探测
    result, err := w.probeManager.prober.probe(w.probeType, w.pod, status, w.container, w.containerID)

    // 7. 更新 metric，判断连续失败/成功次数是否达到阈值
    if result == results.Failure && w.resultRun < int(w.spec.FailureThreshold) { return true }
    if result == results.Success && w.resultRun < int(w.spec.SuccessThreshold) { return true }

    // 8. 达到阈值 → 写入 resultsManager
    w.resultsManager.Set(w.containerID, result, w.pod)
    return true
}
```

### resultsManager.updates → syncLoopIteration 闭环

`resultsManager.Set` 向 `updates chan Update` 写入结果，这个 channel 正是 `syncLoopIteration` 中监听的三个健康检查 channel 之一：

```go
// syncLoopIteration 中（节选）
case update := <-kl.livenessManager.Updates():
    if update.Result == proberesults.Failure {
        handleProbeSync(kl, update, handler, "liveness", "unhealthy")
    }
case update := <-kl.readinessManager.Updates():
    ready := update.Result == proberesults.Success
    kl.statusManager.SetContainerReadiness(update.PodUID, update.ContainerID, ready)
    handleProbeSync(kl, update, handler, "readiness", status)
case update := <-kl.startupManager.Updates():
    started := update.Result == proberesults.Success
    kl.statusManager.SetContainerStartup(update.PodUID, update.ContainerID, started)
    handleProbeSync(kl, update, handler, "startup", status)
```

- **liveness 失败**：`handleProbeSync` → `dispatchWork` → 重启容器
- **readiness 变化**：`SetContainerReadiness` 更新 statusManager，由 Endpoints 控制器决定是否从 Service 摘除
- **startup 完成**：`SetContainerStartup` 标记启动完成，liveness/readiness 才开始生效
