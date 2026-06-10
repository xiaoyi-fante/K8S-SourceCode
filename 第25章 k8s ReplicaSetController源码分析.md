# 第25章 k8s ReplicaSetController 源码分析

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 25 章 — k8s ReplicaSetController 源码分析
> **源码入口**: `pkg/controller/replicaset/replica_set.go`

---

## 核心机制一览

1. **ReplicaSet 是 Deployment 与 Pod 之间的桥梁**：Deployment Controller 管理 ReplicaSet 的副本数，ReplicaSet Controller 负责维护 Pod 数量等于 RS 的 `spec.replicas`。两层分工让滚动更新、回滚等操作都落在 Deployment 层，RS 层只专注 Pod 生命周期。

2. **expectations 机制防止重复 sync**：每次 `manageReplicas` 操作 apiserver 之前，先在本地 expectations 缓存中记录预期的 add/del 数量。informer 收到 pod 创建/删除事件后消费计数。下一次 syncLoop 先检查 `SatisfiedExpectations`（add ≤ 0 && del ≤ 0），若未满足则跳过 `manageReplicas`，避免在 apiserver 响应到来前重复发起操作。

3. **burstReplicas 限制单次操作量**：每次 syncLoop 最多创建或删除 `burstReplicas`（默认 500）个 Pod。超出的部分留到下一次 syncLoop 继续处理，防止 apiserver 被单次 RS 变动打爆。

4. **slowStartBatch 慢启动批量创建**：创建 pod 时以 1→2→4→8…的批次增长，每批并发 goroutine。某批失败则立即返回，避免大量并发创建时错误级联放大。

5. **addPod 回调的 expectations 联动**：pod 创建事件到达时，若 pod 有 ownerRef 指向 RS，则调 `rsc.expectations.CreationObserved(rsKey)`（add -1）；pod 删除事件到达时调 `rsc.expectations.DeletionObserved(rsKey)`（del -1）。两个 informer 回调是 expectations 计数消费的唯一入口。

6. **getPodsToDelete 8 维排序**：缩容时不随机删 pod，而是按 6 个维度（节点分配、phase、ready 状态、运行时间、重启次数、创建时间）从"最劣质"到"最优质"排序，优先删差的，保留健康稳定的 pod。

---

## 全章调用链总图

```
NewReplicaSetController                    replica_set.go:112
  └── NewBaseController                    replica_set.go:129
        ├── 注册 rsInformer 回调            addRS / updateRS / deleteRS
        ├── 注册 podInformer 回调           addPod / updatePod / deletePod
        ├── expectations = NewUIDTrackingControllerExpectations
        └── rsc.syncHandler = rsc.syncReplicaSet

Run                                        replica_set.go:177
  ├── WaitForNamedCacheSync（pod + rs）
  └── N × worker goroutine

worker → processNextWorkItem               replica_set.go:514/519
  └── rsc.syncHandler(key) = syncReplicaSet

syncReplicaSet                             replica_set.go:646
  ├── rsLister.Get(key)                    从 informer 本地缓存取 rs
  ├── SatisfiedExpectations?               未满足 → 只更新 status，跳过 manageReplicas
  ├── getPodMapForRS / ClaimPods           过滤 + 认领 pod
  ├── manageReplicas(filteredPods, rs)     核心：扩/缩 pod
  └── calculateStatus → UpdateStatus      更新 rs.Status

manageReplicas                             replica_set.go:541
  ├── diff = len(filteredPods) - rs.Spec.Replicas
  ├── diff < 0 → 创建
  │     ├── diff = max(diff, -burstReplicas)
  │     ├── expectations.ExpectCreations(rsKey, diff)
  │     └── slowStartBatch → podControl.CreatePods（goroutine 批）
  └── diff > 0 → 删除
        ├── diff = min(diff, burstReplicas)
        ├── getPodsToDelete → 选出最劣质的 pod
        ├── expectations.ExpectDeletions(rsKey, podKeys)
        └── 并发 goroutine → podControl.DeletePod
              └── expectations.DeletionObserved(rsKey, podKey)（on success）
```

---

## §01 ReplicaSetController 初始化

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 对外构造函数 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `NewReplicaSetController:112` |
| 内部初始化 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `NewBaseController:129` |
| RS 增/改/删回调 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `addRS:284` / `updateRS:291` / `deleteRS:326` |
| Pod 增/改/删回调 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `addPod:356` / `updatePod:399` / `deletePod:473` |
| 孤儿 pod 归属查询 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `getPodReplicaSets:230` |

### NewReplicaSetController / NewBaseController

```go
// pkg/controller/replicaset/replica_set.go:112
func NewReplicaSetController(rsInformer, podInformer, kubeClient, burstReplicas) *ReplicaSetController {
    return NewBaseController(rsInformer, podInformer, kubeClient, burstReplicas,
        apps.SchemeGroupVersion.WithKind("ReplicaSet"),
        "replicaset_controller",
        "replicaset",
        controller.RealPodControl{KubeClient: kubeClient, ...},
    )
}
```

`NewBaseController` 完成以下工作：

- 注册 metrics（`ratelimiter.RegisterMetricAndTrackRateLimiterUsage`）
- 初始化 `ReplicaSetController` 结构体，核心字段：
  - `expectations`：`controller.NewUIDTrackingControllerExpectations(controller.NewControllerExpectations())`，用于跟踪每个 RS 的预期 pod add/del 数量
  - `queue`：`workqueue.NewNamedRateLimitingQueue`，限速工作队列
  - `podControl`：`PodControlInterface`，封装 pod 的 create/delete/patch 操作
- `rsc.syncHandler = rsc.syncReplicaSet`

**PodControlInterface** 是核心接口，定义：

```go
type PodControlInterface interface {
    CreatePods(namespace string, template *v1.PodTemplateSpec, object runtime.Object, ...) error
    CreatePodsWithGenerateName(namespace string, template *v1.PodTemplateSpec, ...) error
    DeletePod(namespace string, podID string, object runtime.Object) error
    PatchPod(namespace, name string, data []byte) error
}
```

### rsInformer 事件回调

**addRS**（:284）：只是将 rs 入队，触发 sync。

**updateRS**（:291）：对比新老 rs 的 UID，若 UID 变化（RS 被同名重建），调 `rsc.deleteRS` 删除老 rs 的 expectations 缓存，再将新 rs 入队。只有 `spec.replicas` 变化时记录日志。

**deleteRS**（:326）：删除 expectations 中该 rs 的缓存（`rsc.expectations.DeleteExpectations(key)`），然后入队。这里有个细节：如果 obj 不是 `*apps.ReplicaSet` 类型，说明对象已经是 tombstone（`cache.DeletedFinalStateUnknown`），需要从 tombstone 中取出真正的 RS 对象。

### podInformer 事件回调

pod 的 informer 回调比 rs 复杂，因为 pod 的变化要反向映射到 RS：

**addPod**（:356）：

```
pod 有 ownerRef?
  ├── 是 → resolveControllerRef 取到 RS
  │          rsc.expectations.CreationObserved(rsKey)  // add -1
  │          rsc.queue.Add(rsKey)
  │          return
  └── 否（孤儿 pod）→ getPodReplicaSets 遍历所有 RS 看哪个愿意收养
                       for _, rs := range rss { rsc.enqueueRS(rs) }
```

`CreationObserved` 让 expectations 的 add 计数 -1，表示"我们期望的创建事件已经收到了一个"。

**updatePod**（:399）：

1. 判断版本变化（`ResourceVersion` 相同则跳过）
2. pod 处于删除中（`DeletionTimestamp != nil`）→ 调 `deletePod` 逻辑
3. `labelChanged || controllerRefChanged`：若标签或 ownerRef 变化，同步旧 RS（出队再入队让旧 RS 认识到 pod 离开了）
4. 将新 RS 入队
5. `MinReadySeconds` 逻辑：如果 pod 刚变为 ready，要在 `MinReadySeconds` 秒后再次入队检查（`rsc.queue.AddAfter(rsKey, time.Duration(minReadySeconds)*time.Second)`），确保 RS 在 pod 稳定后才更新 `ReadyReplicas`

**deletePod**（:473）：消费 expectations 中 del 计数（`DeletionObserved`），逻辑在 `addPod` 中已分析；最后赋值 `podLister`。

---

## §02 syncReplicaSet 核心流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 同步入口 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `syncReplicaSet:646` |
| Run / worker | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `Run:177` / `worker:514` |
| expectations 满足判断 | [controller_utils.go](kubernetes/pkg/controller/controller_utils.go) | `SatisfiedExpectations:186` |
| Fulfilled 定义 | [controller_utils.go](kubernetes/pkg/controller/controller_utils.go) | `Fulfilled:280` |
| isExpired 定义 | [controller_utils.go](kubernetes/pkg/controller/controller_utils.go) | `isExpired:216` |
| manageReplicas | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `manageReplicas:541` |
| 慢启动批量创建 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `slowStartBatch:741` |
| 缩容 pod 筛选 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `getPodsToDelete:800` |
| 缩容排序 | [controller_utils.go](kubernetes/pkg/controller/controller_utils.go) | `ActivePodsWithRanks.Less:844` |

### Run 和 worker

```go
// pkg/controller/replicaset/replica_set.go:177
func (rsc *ReplicaSetController) Run(workers int, stopCh <-chan struct{}) {
    if !cache.WaitForNamedCacheSync(rsc.Kind, stopCh, rsc.podListerSynced, rsc.rsListerSynced) {
        return
    }
    for i := 0; i < workers; i++ {
        go wait.Until(rsc.worker, time.Second, stopCh)
    }
    <-stopCh
}
```

标准的两步走：等待 informer 就绪 → 启动多个 worker。

**processNextWorkItem**（:519）：无错误时调 `queue.Forget(key)` 清除限速记录；有错误时调 `queue.AddRateLimited(key)` 带限速重试，并用 `utilruntime.HandleError` 记录。

### syncReplicaSet 主体

```go
// pkg/controller/replicaset/replica_set.go:646
func (rsc *ReplicaSetController) syncReplicaSet(key string) error {
    namespace, name, _ := cache.SplitMetaNamespaceKey(key)
    rs, err := rsc.rsLister.ReplicaSets(namespace).Get(name)
    if apierrors.IsNotFound(err) {
        rsc.expectations.DeleteExpectations(key)  // RS 已删，清除 expectations
        return nil
    }
    rsNeedsSync := rsc.expectations.SatisfiedExpectations(key)

    // 过滤 + 认领 pod
    selector, _ := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
    allPods, _ := rsc.podLister.Pods(rs.Namespace).List(labels.Everything())
    filteredPods := controller.FilterActivePods(allPods)
    filteredPods, _ = rsc.claimPods(rs, selector, filteredPods)

    var manageReplicasErr error
    if rsNeedsSync && rs.DeletionTimestamp == nil {
        manageReplicasErr = rsc.manageReplicas(filteredPods, rs)
    }

    // 计算 newStatus 并更新
    newStatus := calculateStatus(rs, filteredPods, manageReplicasErr)
    updatedRS, _ := updateReplicaSetStatus(rsc.kubeClient.AppsV1().ReplicaSets(rs.Namespace), rs, newStatus)
    // 若 MinReadySeconds > 0 且 RS 未完全就绪，则延迟重入队
    if manageReplicasErr == nil && updatedRS.Spec.Replicas != nil &&
        *(updatedRS.Spec.Replicas) > updatedRS.Status.AvailableReplicas {
        rsc.queue.AddAfter(key, time.Duration(rs.Spec.MinReadySeconds)*time.Second)
    }
    return manageReplicasErr
}
```

### expectations 机制详解

expectations 是一个本地缓存（`sync.Map`），key 是 RS 的 namespace/name，value 是 `ControlleeExpectations`：

```go
type ControlleeExpectations struct {
    add       int64   // 预期还未收到的 pod add 事件数
    del       int64   // 预期还未收到的 pod del 事件数
    timestamp time.Time
}
```

**SatisfiedExpectations**（controller_utils.go:186）：

```
查 expectations 缓存
  ├── 缓存不存在（新 RS）→ return true（允许 sync）
  ├── 缓存过期（> 5min）→ return true（防止永久阻塞）
  └── Fulfilled?（add ≤ 0 && del ≤ 0）
        ├── 是 → return true
        └── 否 → return false（跳过 manageReplicas）
```

这个机制解决了一个竞态问题：`manageReplicas` 调用 apiserver 创建 pod 后，pod 尚未触发 informer 事件，下一次 syncLoop 就又来了。没有 expectations，controller 会认为副本数还不够，再发起一次创建。有了 expectations，第二次 sync 看到 add 计数还大于 0，就跳过，等 pod 事件真正到来后再 sync。

**5 分钟过期的设计意图**：若 pod 事件因某种原因永远没来（网络、节点宕机），expectations 不能无限阻塞 sync。5 分钟过期后强制放行，让 syncReplicaSet 重新观察实际状态并修正。

### manageReplicas 扩容路径

```go
// pkg/controller/replicaset/replica_set.go:541
if diff < 0 {  // 副本不足，需要创建
    diff *= -1
    if diff > rsc.burstReplicas { diff = rsc.burstReplicas }  // 限速

    rsc.expectations.ExpectCreations(rsKey, diff)  // 预设 add 计数
    successfulCreations, err := slowStartBatch(diff, controller.SlowStartInitialBatchSize, func() error {
        return rsc.podControl.CreatePods(rs.Namespace, &rs.Spec.Template, rs, metav1.NewControllerRef(...))
    })
    // 若部分失败：skippedPods = diff - successfulCreations
    // 调 rsc.expectations.CreationObserved 减去 skipped 数，确保 expectations 准确
    if skippedPods := diff - successfulCreations; skippedPods > 0 {
        for i := 0; i < skippedPods; i++ {
            rsc.expectations.CreationObserved(rsKey)
        }
    }
}
```

### slowStartBatch（replica_set.go:741）

```go
func slowStartBatch(count int, initialBatchSize int, fn func() error) (int, error) {
    remaining := count
    successes := 0
    for batchSize := integer.IntMin(remaining, initialBatchSize); batchSize > 0; batchSize = integer.IntMin(remaining, 2*batchSize) {
        // 每批并发启动 batchSize 个 goroutine
        errCh := make(chan error, batchSize)
        var wg sync.WaitGroup
        wg.Add(batchSize)
        for i := 0; i < batchSize; i++ {
            go func() {
                defer wg.Done()
                if err := fn(); err != nil { errCh <- err }
            }()
        }
        wg.Wait()
        curSuccesses := batchSize - len(errCh)
        successes += curSuccesses
        if len(errCh) > 0 { return successes, <-errCh }  // 有失败立即返回
        remaining -= batchSize
    }
    return successes, nil
}
```

批次大小：1 → 2 → 4 → 8 → …（每次翻倍），直到 count。若某批有任何错误，立即停止后续批次。这样能快速发现 pod 创建是否系统性失败（如 namespace 被终止），而不是傻等全部完成。

### manageReplicas 缩容路径

```go
} else if diff > 0 {  // 副本过多，需要删除
    if diff > rsc.burstReplicas { diff = rsc.burstReplicas }

    // getIndirectlyRelatedPods：找同 namespace 下被其他 RS 管理但与本 RS 有关联的 pod，用于缩容排序参考
    relatedPods, _ := rsc.getIndirectlyRelatedPods(rs)
    podsToDelete := getPodsToDelete(filteredPods, relatedPods, diff)

    rsc.expectations.ExpectDeletions(rsKey, getPodKeys(podsToDelete))

    errCh := make(chan error, diff)
    var wg sync.WaitGroup
    wg.Add(diff)
    for _, pod := range podsToDelete {
        go func(targetPod *v1.Pod) {
            defer wg.Done()
            if err := rsc.podControl.DeletePod(rs.Namespace, targetPod.Name, rs); err != nil {
                // 删除失败时主动消费 expectations，防止永久阻塞
                rsc.expectations.DeletionObserved(rsKey, getPodKey(targetPod))
                errCh <- err
            }
        }(pod)
    }
    wg.Wait()
}
```

删除是完全并发的（无 slowStart），因为删除失败的副作用远小于创建失败。

### getPodsToDelete 筛选逻辑（replica_set.go:800）

```go
func getPodsToDelete(filteredPods, relatedPods []*v1.Pod, diff int) []*v1.Pod {
    // diff == len(filteredPods)：全删，不需要排序
    if diff < len(filteredPods) {
        podsWithRanks := getPodsRankedByRelatedPodsOnSameNode(filteredPods, relatedPods)
        sort.Sort(podsWithRanks)  // ActivePodsWithRanks.Less 定义排序规则
        reportSortingDeletionAgeRatioMetric(filteredPods, diff)
    }
    return filteredPods[:diff]
}
```

**ActivePodsWithRanks.Less 排序规则**（6 个维度，优先级从高到低）：

| 优先级 | 判断维度 | 删除哪边 |
|--------|---------|---------|
| 1 | 节点分配 | Unassigned < assigned（未调度的先删）|
| 2 | Pod phase | PodPending < PodUnknown < PodRunning（状态越差先删）|
| 3 | Ready 状态 | Not ready < ready（不健康的先删）|
| 4（ready 时）| 运行时间 | `empty time < less time < more time`（运行时间越短先删）|
| 5 | 重启次数 | higher restarts < lower restarts（重启越多先删）|
| 6 | 创建时间 | Empty creation time < newer < older（越新的先删）|

还有一个隐式第 0 维：**同节点 ready pod 数**。若两个 pod 在同一节点，节点上 ready pod 更多的那个优先被删，以平衡各节点的负载。