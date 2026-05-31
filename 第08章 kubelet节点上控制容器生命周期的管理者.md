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
  │   ├─ go kl.volumeManager.Run()             // 异步启动 volumeManager
  │   ├─ go kl.nodeLeaseController.Run()       // 异步启动 lease 心跳
  │   ├─ go kl.syncNodeStatus()               // 异步启动 NodeStatus 心跳
  │   └─ kl.syncLoop()                         // kubelet.go:1852
  │
  ▼ syncLoop() → syncLoopIteration()           // kubelet.go:1926
  │   └─ case v := <-configCh (kubetypes.ADD)
  │         └─ handler.HandlePodAdditions()    // kubelet.go:2084
  │               └─ kl.dispatchWork()         // kubelet.go:2039
  │                     └─ podWorkers.UpdatePod()  ← 异步
  │                           └─ managePodLoop()
  │                                 └─ kl.syncPod()  // kubelet.go:1497
  │
  ▼ syncPod()
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
klet.nodeLease Controller = lease.NewController(
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
