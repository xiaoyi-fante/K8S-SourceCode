# 第26章 k8s DaemonSet 源码分析

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 26 章 — DaemonSet 控制器源码分析
> **源码入口**: `pkg/controller/daemon/daemon_controller.go`

---

## 核心机制一览

1. **"每节点一个 Pod"的实现方式**：DaemonSet 不依赖调度器，而是自己遍历全量 Node 列表，对每个应该运行但还没运行 Pod 的节点直接创建 Pod，并通过 `nodeAffinity` 将 Pod 锁定到目标节点。调度器看到 `nodeAffinity` 后只需确认，不再重新选节点。

2. **四个 Informer 联动**：DaemonSetController 监听 DaemonSet、ControllerRevision（历史版本）、Pod、Node 四类资源变化，均通过同一个工作队列串行执行 `syncDaemonSet`。其中 `nodeInformer.addNode` 最特殊——新节点加入时主动把所有匹配的 DS 入队，触发在新节点上创建 Pod。

3. **`nodeShouldRunDaemonPod` — 三元谓词**：判断某个节点是否应运行某个 DS 的 Pod，返回 `(shouldRun bool, shouldContinueRunning bool)`。内部从 `Predicates` 取三个布尔变量：`fitsNodeName`（NodeName 匹配）、`fitsNodeAffinity`（节点选择器匹配）、`fitsTaints`（污点容忍），三者均满足才调度；已运行的 Pod 只要能容忍 `NoExecute` 污点就继续运行。

4. **expectations 防重复**：和 ReplicaSet 一样，`syncDaemonSet` 检查 `SatisfiedExpectations` 通过后才执行 `manage`，防止 apiserver 响应未回时重复发起创建/删除。

5. **`constructHistory` + ControllerRevision**：每次 sync 都调用 `constructHistory` 构造当前 hash 对应的 `ControllerRevision` 对象（snapshot），并通过 `ClaimControllerRevisions` 认领历史版本，实现多版本管理和回滚基础。

6. **创建操作用慢启动批次**：`syncNodes` 中创建 Pod 使用 `SlowStartInitialBatchSize` 倍增批次（1→2→4→...），跳过的 Pod 主动调 `CreationObserved` 修正 expectations 计数，防止计数泄漏。

7. **滚动更新核心逻辑**：`rollingUpdate` 遍历所有节点计算出两个集合：`oldPodsToDelete`（需要删除的旧 Pod）和 `newNodesToCreate`（需要新建 Pod 的节点），再调 `syncNodes` 执行。控制 `maxSurge`（允许超出节点数同时运行的 Pod）和 `maxUnavailable`（最多多少节点同时不可用）两个参数平衡升级速度和服务可用性。

---

## 全章调用链总图

```
DaemonSetController
  │
  ▼ 四路 Informer 事件 → Queue
  │
  ├─── DaemonSet Informer: addDaemonset / updateDaemonset / deleteDaemonset
  ├─── History Informer:   addHistory / updateHistory / deleteHistory
  ├─── Pod Informer:       addPod / updatePod / deletePod
  └─── Node Informer:      addNode / updateNode
  │
  ▼ runWorker → processNextWorkItem
  │
  ▼ syncDaemonSet (daemon_controller.go:1155)
  │  ├─── 从 dsLister 取 ds，若已删除则 DeleteExpectations，return
  │  ├─── 从 nodeLister 取全量 node
  │  ├─── 判断 selector 是否为空（空则 warn + return nil）
  │  ├─── constructHistory → cur ControllerRevision, old []ControllerRevision
  │  │      └─── snapshot（build ControllerRevision 并 Create apiserver）
  │  │      └─── ClaimControllerRevisions（match/adopt/release 历史版本）
  │  ├─── SatisfiedExpectations 检查
  │  │      ├─── 未满足 → updateDaemonSetStatus（只更新状态，不操作 Pod）
  │  │      └─── 满足  → manage → syncNodes
  │  ├─── updateDaemonSetStatus (daemon_controller.go:1098)
  │  │      └─── 遍历 nodeList，统计 desiredNumberScheduled/currentNumberScheduled
  │  │           /numberReady/numberAvailable/updatedNumberScheduled
  │  └─── 根据更新策略分支
  │         ├─── OnDelete    → 跳过（等用户手动删除）
  │         └─── RollingUpdate → rollingUpdate (update.go:43)
  │
  ▼ manage (daemon_controller.go:914)
  │  ├─── getNodesToDaemonPods → nodeToDaemonPods map
  │  └─── 遍历 nodeList，调 podsShouldBeOnNode
  │         → nodesNeedingDaemonPods / podsToDelete
  │
  ▼ syncNodes (daemon_controller.go:946)
  │  ├─── SetExpectations(createDiff, deleteDiff)
  │  ├─── 创建：SlowStartInitialBatchSize 慢启动 → podControl.CreatePods
  │  │      └─── 每个 Pod 设置 nodeAffinity → 绑定到目标节点
  │  └─── 删除：并发 goroutine → podControl.DeletePod
  │
  ▼ rollingUpdate (update.go:43)
     ├─── updateDaemonSetStatus 计算 maxSurge/maxUnavailable
     ├─── 遍历 nodeToDaemonPods，findUpdatedPodsOnNode → newPod/oldPod
     ├─── 构建 oldPodsToDelete + candidateNewNodes + allowedNewNodes
     └─── syncNodes（oldPodsToDelete, newNodesToCreate）
```

---

## §01 DaemonSet 常见功能

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| DaemonSet 概念与字段含义 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | — |

DaemonSet 确保全部（或满足选择器的）节点上运行一个 Pod 副本。节点加入集群时自动创建 Pod；节点移除时对应 Pod 被回收；删除 DaemonSet 会删除它创建的所有 Pod。

**典型使用场景**：
- 每个节点运行集群守护进程：`kube-proxy`
- 每个节点运行日志收集守护进程：`filebeat`
- 每个节点运行监控守护进程：`node_exporter`

**仅在满足节点条件时运行 Pod**：`spec.template.spec.nodeSelector`、`spec.template.spec.affinity` 控制节点筛选；DaemonSet Controller 将在符合条件的节点上各调度一个 Pod。

**更新策略**：
- `OnDelete`：用户手动删除每个 Pod 后完成更新，controller 不自动操作
- `RollingUpdate`（默认）：controller 自动控制升级进度，需指定 `spec.updateStrategy.rollingUpdate.maxUnavailable`（默认 1）和 `spec.minReadySeconds`（默认 0）

**删除操作**：
- 非级联删除（`--cascade=orphan`）：只删 DS，Pod 保留，新建相同选择算符的 DS 会收养已有 Pod
- 级联删除：删除 DS 下所有 Pod

**回滚**：通过 `controllerRevision` 保存历史版本信息，回滚时将历史 `controllerrevision` 中的信息替换 DS 中 `Spec.Template`。

---

## §02 DaemonSetController 初始化

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 控制器构造 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `NewDaemonSetsController:133` |
| DS Informer 事件回调 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `addDaemonset:225` / `updateDaemonset:231` / `deleteDaemonset:252` |
| History Informer 事件回调 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `addHistory:380` / `updateHistory:414` / `deleteHistory:461` |
| Pod Informer 事件回调 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `addPod:494` / `updatePod:537` / `deletePod:598` |
| Node Informer 事件回调 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `addNode:636` / `updateNode:689` |
| 节点调度判断 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `nodeShouldRunDaemonPod:1252` |

### 架构图

```
DaemonSet Controller
  │
  ├── Worker ──┐
  ├── Worker ──┼──► Queue ◄── event ── DaemonSet Informer
  └── Worker ──┘              ├───── History  Informer
                              ├───── Pod      Informer
                              └───── Node     Informer
```

DaemonSetController 监听四种资源：DaemonSet、ControllerRevision（历史版本）、Pod、Node，变化事件通过同一 Queue 由多个 Worker goroutine 消费，调用 `syncDaemonSet` 完成状态同步。

### `NewDaemonSetsController` 内部结构

```go
// pkg/controller/daemon/daemon_controller.go:133
func NewDaemonSetsController(ctx context.Context, ...) (*DaemonSetsController, bool, error) {
    // event 广播
    eventBroadcaster := record.NewBroadcaster()
    eventBroadcaster.StartStructuredLogging(0)
    eventBroadcaster.StartRecordingToSink(...)

    dsc := &DaemonSetsController{
        kubeClient:    kubeClient,
        eventRecorder: eventBroadcaster.NewRecorder(...),
        podControl: controller.RealPodControl{...},     // 封装 CreatePods/DeletePod
        crControl:  controller.RealControllerRevisionControl{...},  // 封装 ControllerRevision CRUD
        burstReplicas:  BurstReplicas,
        expectations:   controller.NewControllerExpectations(),
        queue:          workqueue.NewNamedRateLimitingQueue(...),
    }
    // ...
}
```

- `podControl`（`PodControlInterface`）：封装 Pod 的 `CreatePods` / `DeletePod` / `PatchPod` 操作
- `crControl`（`ControllerRevisionControlInterface`）：封装 ControllerRevision 的 `PatchControllerRevision`
- `expectations`：防重复 sync 机制，与 ReplicaSetController 相同
- `queue`：限速工作队列

### DaemonSet Informer 事件回调

**`addDaemonset`（:225）**：直接将 DS 入队。

**`updateDaemonset`（:231）**：对比新旧 DS 的 UID，若 UID 变化（同名重建），则先调 `deleteDaemonset` 清除旧 expectations，再将新 DS 入队。

**`deleteDaemonset`（:252）**：从 tombstone 中恢复对象，然后调 `dsc.expectations.DeleteExpectations(key)` 清除缓存，将 DS 入队。

### History Informer 事件回调

**`addHistory`（:380）**：若 `DeletionTimestamp != nil`，调 `deleteHistory` 处理删除；否则根据 `OwnerReferences` 解析所属 DS，有主就同步对应 DS；否则作为孤儿，遍历所有匹配 DS 入队。

**`updateHistory`（:414）**：
1. 对比 `ResourceVersion`，若未变化则跳过（periodic resync）
2. 对比新旧 history 对象的 `ControllerRef`，若变化则同步旧 DS
3. 如果是孤儿，根据标签变化判断是否需要同步匹配 DS

**`deleteHistory`（:461）**：解析 history 对象的 DS 主人，找到就入队同步。

### Pod Informer 事件回调

**`addPod`（:494）**：
- 若 `DeletionTimestamp != nil`（重启时已在删除中的 Pod），转 `deletePod` 处理
- 主流程：解析 Pod 的 `ControllerRef` 找到所属 DS，调 `dsc.expectations.CreationObserved(dsKey)`（add -1），将 DS 入队
- 孤儿 Pod：调 `getDaemonSetsForPod` 找所有匹配的 DS，各入队尝试收养

**`updatePod`（:537）**：
- `ResourceVersion` 不变则跳过
- `DeletionTimestamp` 变化 → 转 `deletePod`
- 主人 DS 发生变化（ControllerRef 变更）→ 同步旧 DS
- 标签变化 → 同步新 DS
- Pod 进入 Ready 后，若 `MinReadySeconds > 0` 则使用 `AddAfter` 延迟入队（等 MinReadySeconds 秒后再 check numberAvailable）

**`deletePod`（:598）**：解析 Pod 所属 DS，调 `dsc.expectations.DeletionObserved(dsKey)`（del -1），将 DS 入队。

### podInformer 特殊之处

为 podInformer 添加了额外的 `nodeName` 索引：

```go
// pkg/controller/daemon/daemon_controller.go
podInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{
    "nodeName": indexByPodNodeName,
})
```

`indexByPodNodeName` 只索引活跃的 Pod（非 Succeeded/Failed 且有 `NodeName`），用于在 `getNodesToDaemonPods` 中高效按节点名查找 Pod，避免全量扫描。

### Node Informer 事件回调

**`addNode`（:636）**：新节点加入时，从本地缓存获取所有 DS，对每个 DS 调 `nodeShouldRunDaemonPod` 判断该节点是否满足运行条件，满足则将 DS 入队。这是 DaemonSet 在新节点自动创建 Pod 的触发点。

**`updateNode`（:689）**：新旧节点的调度结果若发生变化（`shouldRun` 或标签变化），则入队同步对应 DS。

### `nodeShouldRunDaemonPod` 三元谓词

```go
// pkg/controller/daemon/daemon_controller.go:1252
func (dsc *DaemonSetsController) nodeShouldRunDaemonPod(node *v1.Node, ds *apps.DaemonSet) (bool, bool) {
    pod := NewPod(ds, node.Name)

    // 首先判断 NodeName 是否匹配（空字符串表示任意节点）
    if !(ds.Spec.Template.Spec.NodeName == "" || ds.Spec.Template.Spec.NodeName == node.Name) {
        return false, false
    }

    // 从 Predicates 获取三个布尔变量
    taints := node.Spec.Taints
    fitsNodeName, fitsNodeAffinity, fitsTaints := Predicates(pod, node, taints)
    if !fitsNodeName || !fitsNodeAffinity {
        return false, false
    }

    // fitsTaints 为 false 时，ds 仍可在 NoExecute 污点上运行
    if !fitsTaints {
        _, hasUntoleratedTaint := v1helper.FindMatchingUntoleratedTaint(taints, pod.Spec.Tolerations,
            func(t *v1.Taint) bool { return t.Effect == v1.TaintEffectNoExecute })
        return false, !hasUntoleratedTaint
    }
    return true, true
}
```

三个谓词：
- `fitsNodeName`：`NodeName` 为空或与节点名匹配
- `fitsNodeAffinity`：节点选择器（nodeAffinity）匹配
- `fitsTaints`：没有无法容忍的 `NoExecute` / `NoSchedule` 污点

返回值 `(shouldRun, shouldContinueRunning)`：
- `shouldRun=true`：该节点应创建新 Pod
- `shouldContinueRunning=true`：节点上已有的 Pod 应继续运行（即使有 NoExecute 污点，只要能容忍就继续）

---

## §03 DaemonSetController 状态同步

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 同步入口 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `Run:281` / `syncDaemonSet:1155` |
| 历史版本构造 | [update.go](kubernetes/pkg/controller/daemon/update.go) | `constructHistory:237` / `snapshot:459` / `controlledHistories:394` |
| 状态统计更新 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `updateDaemonSetStatus:1098` |

### `Run` 入口

```go
// pkg/controller/daemon/daemon_controller.go:281
func (dsc *DaemonSetsController) Run(workers int, stopCh <-chan struct{}) {
    defer utilruntime.HandleCrash()
    defer dsc.queue.ShutDown()

    // 等待 pod、ds、node 的 informer 同步完成
    if !cache.WaitForNamedCacheSync("daemon sets", stopCh,
        dsc.podStoreSynced, dsc.nodeStoreSynced, ...) {
        return
    }

    for i := 0; i < workers; i++ {
        go wait.Until(dsc.runWorker, time.Second, stopCh)
    }
    // 启动 failedPodsBackoff GC goroutine
    go wait.Until(dsc.failedPodsBackoff.GC, BackoffGCInterval, stopCh)
    <-stopCh
}
```

### `syncDaemonSet` 主流程

```go
// pkg/controller/daemon/daemon_controller.go:1155
func (dsc *DaemonSetsController) syncDaemonSet(key string) error {
    startTime := dsc.failedPodsBackoff.Clock.Now()
    defer func() { klog.V(4).Infof("Finished syncing daemon set %q (%v)", key, ...) }()

    namespace, name, err := cache.SplitMetaNamespaceKey(key)
    // 从 dsLister 本地缓存查找 ds
    ds, err := dsc.dsLister.DaemonSets(namespace).Get(name)
    if apierrors.IsNotFound(err) {
        dsc.expectations.DeleteExpectations(key)
        return nil
    }

    // 从 nodeLister 获取全量 node
    nodeList, err := dsc.nodeLister.List(labels.Everything())

    // 判断 ds 的 pod 选择器为空 → 告警并退出
    everything := metav1.LabelSelector{}
    if reflect.DeepEqual(ds.Spec.Selector, &everything) {
        dsc.eventRecorder.Eventf(ds, ...)
        return nil
    }

    // 获取 ds 的当前 + 历史 ControllerRevision
    cur, old, err := dsc.constructHistory(ds)
    hash := cur.Labels[apps.DefaultDaemonSetUniqueLabelKey]

    // expectations 检查：未满足 → 只更新状态，不操作 Pod
    if !dsc.expectations.SatisfiedExpectations(dsKey) {
        return dsc.updateDaemonSetStatus(ds, nodeList, hash, false)
    }

    err = dsc.manage(ds, nodeList, hash)

    // MinReadySeconds 兜底重入队
    if ds.Spec.MinReadySeconds > 0 && numberReady != numberAvailable {
        dsc.enqueueDaemonSetAfter(ds, time.Duration(ds.Spec.MinReadySeconds)*time.Second)
    }
}
```

### `constructHistory` — 版本管理

```go
// pkg/controller/daemon/update.go:237
func (dsc *DaemonSetsController) constructHistory(ds *apps.DaemonSet) (
    cur *apps.ControllerRevision, old []*apps.ControllerRevision, err error) {

    var histories []*apps.ControllerRevision
    var currentHistories []*apps.ControllerRevision
    histories, err = dsc.controlledHistories(ds)  // 认领所有 ControllerRevision

    // 根据 hash 分为 currentHistories 和 old
    for _, history := range histories {
        // ...
    }

    switch len(currentHistories) {
    case 0:
        // 没有历史记录，通过 snapshot 创建新 ControllerRevision
        cur, err = dsc.snapshot(ds, currRevision)
    default:
        cur, err = dsc.dedupCurHistories(ds, currentHistories)
        // 如果 revision 号需要更新，调 apiserver Update
        if cur.Revision < currRevision {
            toUpdate := cur.DeepCopy()
            toUpdate.Revision = currRevision
            _, err = dsc.kubeClient.AppsV1().ControllerRevisions(ds.Namespace).Update(...)
        }
    }
    return claimed, old, nil
}
```

**`snapshot`（update.go:459）**：构造 `apps.ControllerRevision` 对象，`OwnerReferences` 指向 DS，通过 `hash = controller.ComputeHash(&ds.Spec.Template, ds.Status.CollisionCount)` 生成唯一标签，调 apiserver Create 保存。

**`controlledHistories`（update.go:394）**：使用 `ClaimControllerRevisions`（match/adopt/release 三个闭包），将所有 Label 匹配的 ControllerRevision 认领到这个 DS 名下。

### `updateDaemonSetStatus` — 状态统计

```go
// pkg/controller/daemon/daemon_controller.go:1098
func (dsc *DaemonSetsController) updateDaemonSetStatus(
    ds *apps.DaemonSet, nodeList []*v1.Node, hash string, updateObservedGen bool) error {

    // 构建每个节点上的 DS pod map
    nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ds)

    var desiredNumberScheduled, currentNumberScheduled, numberMisscheduled,
        numberReady, updatedNumberScheduled, numberAvailable int

    for _, node := range nodeList {
        shouldRun, _ := dsc.nodeShouldRunDaemonPod(node, ds)
        scheduled := len(nodeToDaemonPods[node.Name]) > 0

        if shouldRun {
            desiredNumberScheduled++
            if scheduled {
                currentNumberScheduled++
                // Ready 判断
                for _, pod := range nodeToDaemonPods[node.Name] {
                    if podutil.IsPodReady(pod) {
                        numberReady++
                        // Available = Ready + MinReadySeconds 满足
                        if podutil.IsPodAvailable(pod, ds.Spec.MinReadySeconds, metav1.Now()) {
                            numberAvailable++
                        }
                    }
                    // 根据 templateGeneration 或 hash 判断是否已更新
                    if util.IsPodUpdated(pod, hash, generation) {
                        updatedNumberScheduled++
                    }
                }
            }
        } else if scheduled {
            numberMisscheduled++ // 不该运行但实际在运行
        }
    }
    numberUnavailable := desiredNumberScheduled - numberAvailable
    err = storeDaemonSetStatus(dsc.kubeClient.AppsV1().DaemonSets(ds.Namespace), ds, ...)
}
```

各字段含义：
- `DESIRED`（`desiredNumberScheduled`）：应该运行 Pod 的节点数
- `CURRENT`（`currentNumberScheduled`）：实际运行 ds Pod 的节点数
- `READY`（`numberReady`）：Pod 处于 Ready 状态的节点数
- `AVAILABLE`（`numberAvailable`）：Ready 且满足 `MinReadySeconds` 的节点数
- `UP-TO-DATE`（`updatedNumberScheduled`）：Pod 已是最新版本的节点数
- `MISSCHEDULED`（`numberMisscheduled`）：不应运行但实际在运行的节点数（被误调度）

---

## §04 DaemonSetController 创建操作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 节点-Pod 映射管理 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `manage:914` / `getNodesToDaemonPods:749` |
| 节点 Pod 调度决策 | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `podsShouldBeOnNode:795` |
| 创建/删除 Pod | [daemon_controller.go](kubernetes/pkg/controller/daemon/daemon_controller.go) | `syncNodes:946` |

### `manage` — 调度决策

```go
// pkg/controller/daemon/daemon_controller.go:914
func (dsc *DaemonSetsController) manage(ds *apps.DaemonSet, nodeList []*v1.Node, hash string) error {
    // 构建 nodeName → []Pod 的 map
    nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ds)

    var nodesNeedingDaemonPods, podsToDelete []string
    for _, node := range nodeList {
        nodesNeedingDaemonPodsOnNode, podsToDeleteOnNode :=
            dsc.podsShouldBeOnNode(node, nodeToDaemonPods, ds, hash)
        nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, nodesNeedingDaemonPodsOnNode...)
        podsToDelete = append(podsToDelete, podsToDeleteOnNode...)
    }
    // ...
    return dsc.syncNodes(ds, podsToDelete, nodesNeedingDaemonPods, hash)
}
```

**`podsShouldBeOnNode`（:795）**：对每个节点，判断：
1. `shouldRun` 为 true 且节点上没有 Pod → 加入 `nodesNeedingDaemonPods`
2. `shouldContinueRunning` 为 false 且节点有 Pod → 加入 `podsToDelete`
3. 节点上有多余的 Pod（冗余）→ 按优先级选最差的加入 `podsToDelete`
4. 若 Pod 处于 failed 状态并在 backoff 计时中 → 延迟重试

### `syncNodes` — 慢启动创建 + 并发删除

```go
// pkg/controller/daemon/daemon_controller.go:946
func (dsc *DaemonSetsController) syncNodes(ds *apps.DaemonSet,
    podsToDelete, nodesNeedingDaemonPods []string, hash string) error {

    createDiff := len(nodesNeedingDaemonPods)
    deleteDiff := len(podsToDelete)
    // 登记 expectations
    dsc.expectations.SetExpectations(dsKey, createDiff, deleteDiff)

    // 创建：慢启动批次，避免大量 Pod 同时失败
    errCh := make(chan error, createDiff+deleteDiff)
    createWait := sync.WaitGroup{}
    generation, err := util.GetTemplateGeneration(ds)
    template := util.CreatePodTemplate(ds.Spec.Template, generation, hash)
    batchSize := integer.IntMin(createDiff, controller.SlowStartInitialBatchSize)
    for pos := 0; createDiff > pos; batchSize, pos = integer.IntMin(2*batchSize, createDiff-pos), pos+batchSize {
        errorCount := len(errCh)
        createWait.Add(batchSize)
        for i := pos; i < pos+batchSize; i++ {
            go func(ix int) {
                defer createWait.Done()
                podTemplate := template.DeepCopy()
                // 关键：设置 nodeAffinity，将 Pod 绑定到目标节点
                podTemplate.Spec.Affinity = util.ReplaceDaemonSetPodNodeNameNodeAffinity(
                    podTemplate.Spec.Affinity, nodesNeedingDaemonPods[ix])
                err := dsc.podControl.CreatePods(ds.Namespace, podTemplate, ds,
                    metav1.NewControllerRef(ds, ...))
                if err != nil {
                    if apierrors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
                        return
                    }
                    dsc.expectations.CreationObserved(dsKey) // 失败则修正 expectations
                    errCh <- err
                }
            }(i)
        }
        createWait.Wait()
        // 如果这批有错误，停止慢启动（跳过的 Pod 修正 expectations 计数）
        if len(errCh)-errorCount > 0 {
            skipped := batchSize - (len(errCh) - errorCount)
            // ...
            break
        }
    }

    // 删除：并发 goroutine 删除
    deleteWait := sync.WaitGroup{}
    deleteWait.Add(deleteDiff)
    for i := 0; i < deleteDiff; i++ {
        go func(ix int) {
            defer deleteWait.Done()
            if err := dsc.podControl.DeletePod(...); err != nil {
                dsc.expectations.DeletionObserved(dsKey)
                errCh <- err
            }
        }(i)
    }
    deleteWait.Wait()
}
```

**为什么用 `nodeAffinity` 而不是 `nodeName`**：DaemonSet 不经过调度器的 `nodeName` 直接绑定，而是设置 `nodeAffinity`，让调度器正常经过 Filter/Score 流程（包括 resource fit 等检查），只是最终节点已被 affinity 锁定，保留了正常的调度器保护机制（如资源不足时的处理）。

---

## §05 DaemonSetController 滚动更新

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 滚动更新入口 | [update.go](kubernetes/pkg/controller/daemon/update.go) | `rollingUpdate:43` |

`syncDaemonSet` 末尾根据更新策略分支：

```go
// pkg/controller/daemon/daemon_controller.go
switch ds.Spec.UpdateStrategy.Type {
case apps.OnDeleteDaemonSetStrategyType:
    // 不执行，等用户手动删除
case apps.RollingUpdateDaemonSetStrategyType:
    err = dsc.rollingUpdate(ds, nodeList, hash)
}
```

### `rollingUpdate` 逻辑

```go
// pkg/controller/daemon/update.go:43
func (dsc *DaemonSetsController) rollingUpdate(ds *apps.DaemonSet,
    nodeList []*v1.Node, hash string) error {

    // 构建 nodeToDaemonPods map
    nodeToDaemonPods, err := dsc.getNodesToDaemonPods(ds)

    // 计算 maxSurge 和 maxUnavailable
    maxSurge, maxUnavailable, err := dsc.updatedDesiredNodeCounts(ds, nodeList, nodeToDaemonPods)

    var oldPodsToDelete []string
    var candidateNewNodes []string
    var allowedNewNodes []string
    var numSurge int

    for nodeName, pods := range nodeToDaemonPods {
        // 找到节点上的新 Pod 和旧 Pod
        newPod, oldPod, ok := findUpdatedPodsOnNode(ds, pods, hash)

        switch {
        case oldPod == nil && newPod == nil:
            // 节点上没有任何 Pod，不操作（由 manage 处理）
        case newPod == nil:
            // 没有新 Pod，加入候选创建节点
            candidateNewNodes = append(candidateNewNodes, nodeName)
        default:
            // 有新 Pod
            if podutil.IsPodAvailable(newPod, ds.Spec.MinReadySeconds, metav1.Time{Time: now}) {
                // 新 Pod 可用
                if oldPod != nil {
                    oldPodsToDelete = append(oldPodsToDelete, oldPod.Name)
                }
            } else {
                // 新 Pod 不可用，计入 numSurge 或 numUnavailable
                // 允许加入 allowedNewNodes
                allowedNewNodes = append(allowedNewNodes, nodeName)
                numSurge++
            }
        }
    }

    // 如果不分许有多的 Pod 出现（maxSurge == 0）
    if maxSurge == 0 {
        // 对 candidatePodsToDelete 按节点上 DaemonPods 运行情况排序
        // 选出可以删除的旧 Pod（保证不超出 maxUnavailable）
        // ...
        sort.Sort(...)
        if !util.IsPodAvailable(oldPod, ...) {
            oldPodsToDelete = append(oldPodsToDelete, oldPod.Name)
        } else if oldestNewPod == nil {
            // 旧 Pod 可用，新 Pod 不存在 → allowedNewNodes
            allowedNewNodes = append(allowedNewNodes, nodeName)
        }
    }

    // 如果允许 maxSurge > 0：先创建新 Pod，后删除旧 Pod
    remainingSurge := maxSurge - numSurge
    if remainingSurge < 0 {
        remainingSurge = 0
    }
    if max := len(candidateNewNodes); remainingSurge > max {
        remainingSurge = max
    }
    newNodesToCreate := append(allowedNewNodes, candidateNewNodes[:remainingSurge]...)

    return dsc.syncNodes(ds, oldPodsToDelete, newNodesToCreate, hash)
}
```

**`maxSurge = 0` 时的滚动逻辑（最常见）**：

每个 sync 循环中：
1. `manage` 已经确保每个节点有一个 Pod（可能是旧版本）
2. `rollingUpdate` 找到旧 Pod，判断是否可删：旧 Pod 可用 → 加入 `allowedNewNodes`（下轮会创建新 Pod 替换）；旧 Pod 不可用 → 直接加入 `oldPodsToDelete`
3. `syncNodes` 先创建新 Pod，再删旧 Pod（或反过来，取决于 maxSurge/maxUnavailable 配置）

**`maxSurge > 0` 时的逻辑**：

允许在旧 Pod 删除前先创建新 Pod，即节点上暂时有两个 Pod（surge）。新 Pod 变 Available 后再删旧 Pod，减少服务中断时间，代价是短暂多占资源。

**`updatedDesiredNodeCounts`**：计算 `maxSurge` 和 `maxUnavailable` 的绝对值（从百分比或绝对数换算），若两者同时为 0 则默认 `maxUnavailable = 1`。

**`syncNodes` 调用关系**：`rollingUpdate` 最终调 `syncNodes(ds, oldPodsToDelete, newNodesToCreate, hash)`，复用与 `manage` 相同的 Pod 创建/删除逻辑，保持代码路径统一。
