# 第24章 k8s deployment源码解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 24 章 — k8s deployment 源码解读
> **源码入口**: `pkg/controller/deployment/deployment_controller.go`

---

## 核心机制一览

1. **syncDeployment 操作优先级**：`delete > pause > rollback > scale > rollout`。每次 syncDeployment 只执行其中一个分支，严格按优先级从上到下判断，互相排斥。

2. **ReplicaSet 是 Deployment 的载体**：Deployment 自身不管理 Pod，而是通过创建/删除/扩缩 ReplicaSet 来间接管理 Pod。每次滚动更新都会产生一个新 RS，历史 RS 由 `revisionHistoryLimit` 控制保留数量。

3. **删除只更新 metadata，不删 Pod**：客户端触发删除时，只是在 Deployment metadata 中设置 `DeletionTimestamp`。controller 检测到后调用 `syncStatusOnly` 同步状态，真正的 Pod 清理由 GarbageCollector controller 异步完成。

4. **pause/scale 都走 sync 方法**：`pause`（暂停）和 `scale`（扩缩容）都通过 `dc.sync()` 完成，`sync` 内部先检查 scale 是否需要操作，再根据 `d.Spec.Paused` 决定是否清理历史 RS 并更新状态。

5. **scale 三步判断**：① `activeOrLatest` 只有 1 个 → 直接 scale 它；② newRS 已饱和 → 将所有 oldRS 缩为 0；③ 升级途中 → 按比例分配（`nameToSize` map 按 RS 大小比例分配新副本数，余量给最大 RS）。

6. **滚动更新双向驱动**：`rolloutRolling` 先调 `reconcileNewReplicaSet`（扩新 RS）再调 `reconcileOldReplicaSets`（缩旧 RS），两个操作交替推进。过程中始终保证总 Pod 数 ≤ `Replicas + maxSurge`，可用 Pod 数 ≥ `Replicas - maxUnavailable`。

7. **Recreate（暴力新建）**：缩旧 RS 至 0 → 等旧 Pod 全部退出（`oldPodsRunning` 检测 podMap）→ 扩新 RS → 清理 → 更新 status。停机时间长但逻辑简单。

8. **回滚 = 找到目标 revision 对应的 RS，用其 PodTemplate 替换 deployment.Spec.Template**：通过 `d.Annotations[DeprecatedRollbackTo]` 携带目标 revision 号，`rollbackToTemplate` 比对 template 是否相同（相同则跳过），不同则将 RS 的 Template 写回 Deployment spec 并清除回滚 annotation。

---

## 全章调用链总图

```
NewDeploymentController                    deployment_controller.go
  │  注册 dep/rs/pod informer 回调
  │  构造 workqueue、eventRecorder
  ▼
Run → worker → processNextWorkItem         deployment_controller.go:149/460/465
  │
  ▼ dc.syncHandler = dc.syncDeployment
syncDeployment                             deployment_controller.go:568
  ├── getReplicaSetsForDeployment          deployment_controller.go:503
  │     └── ClaimReplicaSets（match/adopt/release）
  ├── getPodMapForDeployment               deployment_controller.go:536
  │     按 RS UID 分组 pod
  │
  ├── [delete]  DeletionTimestamp != nil
  │     └── syncStatusOnly                 sync.go:37
  │           └── calculateStatus → UpdateStatus
  │
  ├── [pause]   d.Spec.Paused
  │     └── dc.sync                        sync.go:49
  │           ├── scale（如有必要）
  │           ├── cleanupDeployment（历史 RS 清理）
  │           └── syncDeploymentStatus
  │
  ├── [rollback] getRollbackTo != nil
  │     └── dc.rollback                    rollback.go:34
  │           └── rollbackToTemplate       rollback.go:77
  │                 └── updateDeploymentAndClearRollbackTo
  │
  ├── [scale]   isScalingEvent
  │     └── dc.sync                        sync.go:49
  │
  └── [rollout]
        ├── RollingUpdate → rolloutRolling  rolling.go:31
        │     ├── reconcileNewReplicaSet    rolling.go:68  (扩新)
        │     ├── reconcileOldReplicaSets   rolling.go:86  (缩旧)
        │     │     ├── cleanupUnhealthyReplicas
        │     │     └── scaleDownOldReplicaSetsForRollingUpdate
        │     └── syncRolloutStatus         progress.go:37
        │
        └── Recreate → rolloutRecreate      recreate.go:28
              ├── scaleDownOldReplicaSetsForRecreate
              ├── oldPodsRunning（等旧 pod 退出）
              ├── scaleUpNewReplicaSetForRecreate
              ├── cleanupDeployment
              └── syncRolloutStatus
```

---

## §01 Deployment 的基本功能

| 读码目标 | 源文件（可点击） | 入口 |
|---------|----------------|------|
| Deployment 控制器结构 | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `NewDeploymentController:101` |
| RS 操作工具函数 | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | — |

Deployment 是 Kubernetes 中最常用的工作负载资源，核心特性：

- **声明式更新**：用户只描述期望状态（副本数、镜像版本），controller 自动驱动收敛
- **滚动更新**（`RollingUpdate`）：默认策略，新旧版本交替，`maxSurge` 控制超出期望的 Pod 上限，`maxUnavailable` 控制不可用 Pod 下限
- **暴力新建**（`Recreate`）：先删所有旧 Pod，再建新 Pod；停机时间长
- **暂停与恢复**：`spec.paused = true` 暂停 rollout，期间 scale 操作仍然生效
- **回滚**：通过在 `annotations` 中写入 `DeprecatedRollbackTo` 触发，controller 检测后执行回滚
- **版本历史**：每次更新产生一个新 RS，`spec.revisionHistoryLimit`（默认 10）控制保留的历史 RS 数量；历史 RS 是回滚的数据来源

Deployment 不直接管理 Pod，而是通过 ReplicaSet 间接管理。每个 RS 对应一个 PodTemplate 版本，RS 的 `spec.replicas` 决定该版本运行多少个 Pod。

---

## §02 DeploymentController 初始化

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| controller 构造 | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `NewDeploymentController:101` |
| 启动 worker | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `Run:149` |
| worker 消费队列 | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `worker:460` / `processNextWorkItem:465` |
| 错误处理 | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `handleErr:478` |
| 获取关联 RS | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `getReplicaSetsForDeployment:503` |
| 获取 podMap | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `getPodMapForDeployment:536` |

### NewDeploymentController 构造

```go
// pkg/controller/deployment/deployment_controller.go
func NewDeploymentController(dInformer ..., rsInformer ..., podInformer ..., client ...) (*DeploymentController, error) {
```

- 注册 dep/rs/pod 三个 informer 的 EventHandler，将变更的 key 入队
- `dc.syncHandler = dc.syncDeployment`：将同步函数赋值给字段，方便测试替换
- `ConcurrentDeploymentSyncs` 默认 5，控制并发 worker 数量（注释标注了 `// Job`，其实是 Deployment）

### Run / worker 流程

```go
// pkg/controller/deployment/deployment_controller.go:149
func (dc *DeploymentController) Run(workers int, stopCh <-chan struct{}) {
    if !cache.WaitForNamedCacheSync("deployment", stopCh, dc.dListerSynced, dc.rsListerSynced, dc.podListerSynced) {
        return
    }
    for i := 0; i < workers; i++ {
        go wait.Until(dc.worker, time.Second, stopCh)
    }
```

- 和其他 controller 一样，先等待 3 个 informer 的 ListerSynced 返回 true，确认本地缓存已就绪
- 然后启动 N 个 worker goroutine，每个 worker 循环调用 `processNextWorkItem`

**handleErr**（deployment_controller.go:478）：若 syncHandler 报错，先判断是否因 namespace 被删除（忽略），然后判断重试次数。不超过 `maxRetries` 则调 `dc.queue.AddRateLimited(key)` 带限速重入队；超过则调 `dc.queue.Forget(key)` 彻底丢弃。

### getReplicaSetsForDeployment（deployment_controller.go:503）

```
从 informer 本地缓存获取同 namespace 的所有 rsList
  │
  ▼ 解析 dep 的标签选择器
  │
  ▼ 初始化 ReplicaSetControllerRefManager (cm)
  │    canAdoptFunc: 直接 GET apiserver 验证 dep 未被删除，UID 一致
  │
  ▼ cm.ClaimReplicaSets(rsList)
       对每个 rs 应用三个方法：
         match  — 标签是否匹配
         adopt  — 调 AdoptReplicaSet（更新 ownerRef）
         release — 调 ReleaseReplicaSet（清除 ownerRef）
       返回 claimed RS 列表
```

`canAdoptFunc` 直接从 apiserver GET 最新 dep 对象，避免用本地缓存的过期数据来判断是否可以认领孤儿 RS，防止"dep 已删但 controller 还在认领 RS"的问题。

### getPodMapForDeployment（deployment_controller.go:536）

从 informer 本地缓存获取该 dep 所在 namespace 的所有 pod，按 RS 的 UID 分组成 `map[types.UID][]*v1.Pod`。代码注释说明了两个用途：

1. 检查 pod 是否已正确打上 `pod-template-hash` 标签
2. 检查 Recreate 策略下，是否还有旧 pod 在运行（`oldPodsRunning` 函数会用到）

---

## §03 syncDeployment 准备工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 同步入口 | [deployment_controller.go](kubernetes/pkg/controller/deployment/deployment_controller.go) | `syncDeployment:568` |
| syncStatusOnly | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `syncStatusOnly:37` |
| 获取 newRS/oldRSs | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `getAllReplicaSetsAndSyncRevision:116` |

### syncDeployment 主体结构

```go
// pkg/controller/deployment/deployment_controller.go:568
func (dc *DeploymentController) syncDeployment(key string) error {
    startTime := time.Now()
    // ...
    deployment, err := dc.dLister.Deployments(namespace).Get(name)  // 从本地缓存取
    d := deployment.DeepCopy()  // 深拷贝，避免污染缓存

    // 标签选择器为空时更新 generation 并返回
    everything := metav1.LabelSelector{}
    if reflect.DeepEqual(d.Spec.Selector, &everything) { ... }

    rsList, err := dc.getReplicaSetsForDeployment(d)
    podMap, err := dc.getPodMapForDeployment(d, rsList)
    // ... 然后按优先级分支处理
```

准备工作三步：① 从 dLister 取 dep（informer 本地缓存）；② `DeepCopy()` 深拷贝；③ `getReplicaSetsForDeployment` + `getPodMapForDeployment`。

### 操作优先级

```go
// 优先级从高到低
delete > pause > rollback > scale > rollout
```

- **delete**（`d.DeletionTimestamp != nil`）→ `syncStatusOnly`：只更新 status，不做任何 mutating 操作，等待 GC 回收
- **pause**（`d.Spec.Paused`）→ `dc.sync`：先 scale，再清理历史 RS，最后更新 status
- **rollback**（`getRollbackTo(d) != nil`）→ `dc.rollback`
- **scale**（`isScalingEvent`）→ `dc.sync`
- **rollout** → `rolloutRolling` 或 `rolloutRecreate`

### syncStatusOnly 计算 dep status

```go
// pkg/controller/deployment/sync.go:37
func (dc *DeploymentController) syncStatusOnly(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
    allRSs := append(oldRSs, newRS)
    return dc.syncDeploymentStatus(allRSs, newRS, d)
}
```

`calculateStatus`（被 `syncDeploymentStatus` 调用）遍历所有 RS 统计 `Replicas`、`ReadyReplicas`、`AvailableReplicas`、`UpdatedReplicas`，并设置 `DeploymentAvailable` / `MinAvailability` condition，最后 `UpdateStatus` 写回 apiserver。

---

## §04 syncDeployment 之删除、暂停、回滚

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 暂停/scale 公共路径 | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `sync:49` |
| 清理历史 RS | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `cleanupDeployment:435` |
| 回滚入口 | [rollback.go](kubernetes/pkg/controller/deployment/rollback.go) | `rollback:34` |
| 判断 rollback | [rollback.go](kubernetes/pkg/controller/deployment/rollback.go) | `getRollbackTo:123` |
| 执行 rollback | [rollback.go](kubernetes/pkg/controller/deployment/rollback.go) | `rollbackToTemplate:77` |
| 清除回滚 annotation | [rollback.go](kubernetes/pkg/controller/deployment/rollback.go) | `updateDeploymentAndClearRollbackTo:115` |
| 更新 RS annotation | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | `SetDeploymentAnnotationsTo:334` |

### 删除

删除操作只修改 metadata（设置 `DeletionTimestamp`），controller 检测到后：

```go
if d.DeletionTimestamp != nil {
    return dc.syncStatusOnly(d, rsList)
}
```

真正的 RS 和 Pod 清理由 kube-controller-manager 中的 **GarbageCollector controller** 通过 ownerRef 级联删除完成，DeploymentController 不参与删除逻辑。

`DeletionTimestamp` 也是 orphan/background/foreground 三种删除模式的共同入口标志。

### 暂停与恢复（sync 方法）

```go
// pkg/controller/deployment/sync.go:49
func (dc *DeploymentController) sync(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
    if err := dc.scale(d, newRS, oldRSs); err != nil { return err }

    // 若暂停且无 rollback 在途，清理超出 revisionHistoryLimit 的旧 RS
    if d.Spec.Paused && getRollbackTo(d) == nil {
        if err := dc.cleanupDeployment(oldRSs, d); err != nil { return err }
    }

    allRSs := append(oldRSs, newRS)
    return dc.syncDeploymentStatus(allRSs, newRS, d)
}
```

关键细节：**暂停后不是立即停止所有操作**。`sync` 中会先执行 `scale`，所以 pause 后 scale 操作仍然生效。例如，当滚动更新的第一个周期完成后触发 pause，此时滚动更新不会立即停止，而是等到当前周期完成后才会停到暂停状态。

### cleanupDeployment（sync.go:435）

```go
func (dc *DeploymentController) cleanupDeployment(oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) error {
    if !deploymentutil.HasRevisionHistoryLimit(deployment) {
        return nil
    }
    // 过滤掉正在删除的 RS（DeletionTimestamp != nil）
    aliveFilter := func(rs *apps.ReplicaSet) bool { return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil }
    cleanableRSes := controller.FilterReplicaSets(oldRSs, aliveFilter)
    diff := int32(len(cleanableRSes)) - *deployment.Spec.RevisionHistoryLimit
    if diff <= 0 { return nil }
    // 按 revision 号升序排列，最老的在前
    sort.Sort(deploymentutil.ReplicaSetsByRevision(cleanableRSes))
    // 遍历 diff 个 RS，调用 apiserver 删除
    for i := int32(0); i < diff; i++ {
        rs := cleanableRSes[i]
        if rs.Status.Replicas != 0 || *(rs.Spec.Replicas) != 0 || rs.Generation > rs.Status.ObservedGeneration {
            continue  // 副本数不为 0 的 RS 跳过，不删
        }
        dc.client.AppsV1().ReplicaSets(rs.Namespace).Delete(...)
    }
}
```

### 回滚

**触发机制**：客户端在 dep 的 `annotations["deprecated.deployment.rollback.to"]` 中写入目标 revision 号，controller 通过 `getRollbackTo(d)` 读取。

```go
// rollback.go:34
func (dc *DeploymentController) rollback(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
    newRS, allOldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, true)
    rollbackTo := getRollbackTo(d)

    if rollbackTo.Revision == 0 {
        // revision 为 0 表示回滚到上一个版本
        rollbackTo.Revision = deploymentutil.LastRevision(allRSs)
    }

    for _, rs := range allRSs {
        v := deploymentutil.Revision(rs)
        if v == rollbackTo.Revision {
            // 找到目标 RS，用其 template 替换 deployment.Spec.Template
            performedRollback, err := dc.rollbackToTemplate(d, rs)
            ...
        }
    }
    // 找不到目标版本 → emitRollbackRevisionNotFoundEvent
    return dc.updateDeploymentAndClearRollbackTo(d)
}
```

**rollbackToTemplate**（rollback.go:77）：比对 `d.Spec.Template` 与 `rs.Spec.Template` 是否相等（用 `EqualIgnoreHash`）。若相同，说明当前版本即目标版本，发出 `RollbackTemplateUnchanged` event 跳过；否则将 RS 的 Template 拷贝回 Deployment spec，设置 `performedRollback = true`。

最后调 `updateDeploymentAndClearRollbackTo` → `SetDeploymentAnnotationsTo`（deployment_util.go:334）清除 `DeprecatedRollbackTo` annotation，并将目标 RS 的 annotations 复制到 dep，然后更新 dep spec。

---

## §05 DeploymentController 之扩缩容

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 判断是否为 scale 事件 | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `isScalingEvent:528` |
| scale 主逻辑 | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `scale:298` |
| RS 扩缩 + annotation 更新 | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `scaleReplicaSetAndRecordEvent:396` |
| RS 副本数写 apiserver | [sync.go](kubernetes/pkg/controller/deployment/sync.go) | `scaleReplicaSet:411` |
| 找当前活跃 RS | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | `FindActiveOrLatest:355` |
| 判断 newRS 是否饱和 | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | `IsSaturated:847` |

### isScalingEvent（sync.go:528）

遍历所有 RS，找出 `rs.Spec.Replicas > 0` 的活跃 RS，过滤掉 `FilterActiveReplicaSets` 无效的，最后比对 RS 的 `DesiredReplicasAnnotation` 与 dep 当前期望副本数，若有不一致则认定为 scale 事件。

### scale 三步判断（sync.go:298）

```
                 scale(d, newRS, oldRSs)
                        │
          ┌─────────────┴─────────────────┐
          │ FindActiveOrLatest             │
          │ 找出当前活跃或最新的 RS         │
          └────────────┬──────────────────┘
                       │
         ┌─────────────┴──────────────────┐
         │判断01: activeOrLatest 只有 1 个?│
         └─────────────┬──────────────────┘
               是 ──── scaleReplicaSetAndRecordEvent(activeOrLatest, d.Spec.Replicas)
               否 ──── 继续
                       │
         ┌─────────────┴──────────────────┐
         │判断02: IsSaturated(d, newRS)?   │
         │ newRS 期望数 == dep 期望数?      │
         └─────────────┬──────────────────┘
               是 ──── 将所有 oldRS scale 为 0
               否 ──── 继续
                       │
         ┌─────────────┴──────────────────┐
         │判断03: 升级途中，按比例分配       │
         │ nameToSize map 按比例分配新副本  │
         │ 余量给最大的 RS                  │
         └──────────────────────────────────┘
```

**按比例分配（判断03）**的逻辑：

```go
// 计算每个 RS 按比例应该有多少副本
proportion := GetProportion(rs, *deployment, deploymentReplicasToAdd)
nameToSize[rs.Name] = *(rs.Spec.Replicas) + proportion
// 余量：deploymentReplicasAdded 是已分配的总数
// 最大的 RS 吸收剩余余量
```

**scaleReplicaSet**（sync.go:411）：更新 RS 的 `DesiredReplicasAnnotation`（记录 dep 当前期望副本数），同时将 `rs.Spec.Replicas` 改为 newScale，调用 `dc.client.AppsV1().ReplicaSets(...).Update()` 写 apiserver，RS controller 看到后驱动 Pod 创建/删除。

---

## §06 DeploymentController 之滚动更新

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 滚动更新入口 | [rolling.go](kubernetes/pkg/controller/deployment/rolling.go) | `rolloutRolling:31` |
| 扩新 RS | [rolling.go](kubernetes/pkg/controller/deployment/rolling.go) | `reconcileNewReplicaSet:68` |
| 缩旧 RS | [rolling.go](kubernetes/pkg/controller/deployment/rolling.go) | `reconcileOldReplicaSets:86` |
| 清理不健康副本 | [rolling.go](kubernetes/pkg/controller/deployment/rolling.go) | `cleanupUnhealthyReplicas:155` |
| 计算 scaleDown 量 | [rolling.go](kubernetes/pkg/controller/deployment/rolling.go) | `scaleDownOldReplicaSetsForRollingUpdate:192` |
| 更新 rollout status | [progress.go](kubernetes/pkg/controller/deployment/progress.go) | `syncRolloutStatus:37` |
| maxUnavailable 计算 | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | `MaxUnavailable:435` |
| maxSurge 计算 | [deployment_util.go](kubernetes/pkg/controller/deployment/util/deployment_util.go) | `MaxSurge:456` |

### rolloutRolling 主流程

```go
// pkg/controller/deployment/rolling.go:31
func (dc *DeploymentController) rolloutRolling(d *apps.Deployment, rsList []*apps.ReplicaSet) error {
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, true)
    allRSs := append(oldRSs, newRS)

    // Step 1: 扩新 RS
    scaledUp, err := dc.reconcileNewReplicaSet(allRSs, newRS, d)
    if scaledUp { return dc.syncRolloutStatus(allRSs, newRS, d) }

    // Step 2: 缩旧 RS
    scaledDown, err := dc.reconcileOldReplicaSets(allRSs, controller.FilterActiveReplicaSets(oldRSs), newRS, d)
    if scaledDown { return dc.syncRolloutStatus(allRSs, newRS, d) }

    // Step 3: 判断是否完成
    if deploymentutil.DeploymentComplete(d, &d.Status) {
        if err := dc.cleanupDeployment(oldRSs, d); err != nil { return err }
    }
    return dc.syncRolloutStatus(allRSs, newRS, d)
}
```

每个 syncLoop 内，**扩新或缩旧只执行一个**（scaledUp/scaledDown 任一为 true 就提前 return），下一个 syncLoop 继续推进，通过多次迭代完成完整的滚动更新。

### reconcileNewReplicaSet（rolling.go:68）

```go
func (dc *DeploymentController) reconcileNewReplicaSet(allRSs, newRS, deployment) (bool, error) {
    if *(newRS.Spec.Replicas) == *(deployment.Spec.Replicas) { return false, nil }  // 已达期望，不操作

    if *(newRS.Spec.Replicas) > *(deployment.Spec.Replicas) {
        // 新 RS 比期望多 → scale down 到期望数
        scaled, _, err := dc.scaleReplicaSetAndRecordEvent(newRS, *(deployment.Spec.Replicas), deployment)
        return scaled, err
    }
    // 计算可以扩多少：不超过 maxSurge
    newReplicasCount := deploymentutil.NewRSNewReplicas(deployment, allRSs, newRS)
    scaled, _, err := dc.scaleReplicaSetAndRecordEvent(newRS, newReplicasCount, deployment)
    return scaled, err
}
```

### reconcileOldReplicaSets（rolling.go:86）

```
oldPodsCount = 所有旧 RS 的副本总数
allPodsCount = 所有 RS 的副本总数
maxUnavailable = deployment.Spec.Replicas * 25%（向下取整）
minAvailable = deployment.Spec.Replicas - maxUnavailable

计算最多可以缩掉几个：
  newRSUnavailablePodCount = newRS.Spec.Replicas - newRS.Status.AvailableReplicas
  maxScaledDown = allPodsCount - minAvailable - newRSUnavailablePodCount

先清理不健康的旧 RS（cleanupUnhealthyReplicas）
再按 scaleDownOldReplicaSetsForRollingUpdate 缩健康的旧 RS
```

**cleanupUnhealthyReplicas**：优先删除旧 RS 中不健康（非 available）的 pod 对应的 RS 副本，释放"不可用配额"让新版本有空间扩容。

**scaleDownOldReplicaSetsForRollingUpdate**（rolling.go:192）：按 RS 创建时间排序（**先新后旧**），依次缩容，直到缩完 maxScaledDown 个。每次缩容都调 `scaleReplicaSetAndRecordEvent`，同时 double check 当前 available pod 数是否仍满足 minAvailable。

### 滚动更新数值示例

初始状态：10 副本 nginx 1.8，更新到 1.9。`maxSurge=25%→3`，`maxUnavailable=25%→2`：

```
初始：oldRS=10, newRS=0, total=10, available=10
minAvailable = 10-2 = 8,  maxTotal = 10+3 = 13

第1轮扩新：newRS=3 (10+3=13，达上限)
第1轮缩旧：available=10+0=10, maxScaledDown=10-8-0=2, oldRS→8
...
（交替推进）
...
最终：oldRS=0, newRS=10
```

### syncRolloutStatus（progress.go:37）

计算新的 `DeploymentStatus`，判断是否设置 `DeploymentProgressing` condition（消息为 "Replica set has successfully progressed"），然后 `UpdateStatus` 写 apiserver。这是每次 scale 操作后同步状态的必经路径。

---

## §07 DeploymentController 之暴力新建（Recreate）

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Recreate 入口 | [recreate.go](kubernetes/pkg/controller/deployment/recreate.go) | `rolloutRecreate:28` |
| 缩旧 RS 至 0 | [recreate.go](kubernetes/pkg/controller/deployment/recreate.go) | `scaleDownOldReplicaSetsForRecreate:77` |
| 等旧 pod 退出 | [recreate.go](kubernetes/pkg/controller/deployment/recreate.go) | `oldPodsRunning:98` |
| 扩新 RS | [recreate.go](kubernetes/pkg/controller/deployment/recreate.go) | `scaleUpNewReplicaSetForRecreate:126` |

### rolloutRecreate 流程

```go
// pkg/controller/deployment/recreate.go:28
func (dc *DeploymentController) rolloutRecreate(d *apps.Deployment, rsList []*apps.ReplicaSet, podMap ...) error {
    newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(d, rsList, false)
    allRSs := append(oldRSs, newRS)
    activeOldRSs := controller.FilterActiveReplicaSets(oldRSs)

    // Step 1: 将所有旧 RS 缩为 0
    scaledDown, err := dc.scaleDownOldReplicaSetsForRecreate(activeOldRSs, d)
    if scaledDown { return dc.syncRolloutStatus(allRSs, newRS, d) }

    // Step 2: 等待旧 pod 全部退出
    if oldPodsRunning(newRS, oldRSs, podMap) {
        return dc.syncRolloutStatus(allRSs, newRS, d)
    }

    // Step 3: 扩新 RS 到期望副本数
    if _, err := dc.scaleUpNewReplicaSetForRecreate(newRS, d); err != nil { return err }

    // Step 4: 部署完成后清理历史 RS
    if util.DeploymentComplete(d, &d.Status) {
        if err := dc.cleanupDeployment(oldRSs, d); err != nil { return err }
    }
    return dc.syncRolloutStatus(allRSs, newRS, d)
}
```

### scaleDownOldReplicaSetsForRecreate（recreate.go:77）

遍历所有旧 RS，跳过已经是 0 副本的（`*(rs.Spec.Replicas) == 0`），对其余的调 `scaleReplicaSetAndRecordEvent(rs, 0, deployment)`，底层更新 RS `spec.replicas = 0`。

### oldPodsRunning（recreate.go:98）

```go
func oldPodsRunning(newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet, podMap map[types.UID][]*v1.Pod) bool {
    if oldPods := util.GetActualReplicaCountForReplicaSets(oldRSs); oldPods > 0 { return true }
    for rsUID, podList := range podMap {
        if newRS != nil && newRS.UID == rsUID { continue }  // 跳过新 RS 的 pod
        for _, pod := range podList {
            switch pod.Status.Phase {
            case v1.PodFailed, v1.PodSucceeded:
                continue  // 终态 pod 不算"在运行"
            case v1.PodUnknown:
                return true  // Unknown 状态保守处理，认为还在运行
            default:
                return true  // 其他（Pending/Running）都算还在运行
            }
        }
    }
    return false
}
```

这里用到了 `getPodMapForDeployment` 在 `syncDeployment` 开头构建的 podMap，这是 podMap 被传入的原因——Recreate 需要通过实际 pod 状态而非 RS 副本数来判断旧 pod 是否彻底退出，因为 RS 副本数变 0 后 Pod 可能还在 Terminating。

### scaleUpNewReplicaSetForRecreate（recreate.go:126）

直接将新 RS scale 到 `deployment.Spec.Replicas`：

```go
func (dc *DeploymentController) scaleUpNewReplicaSetForRecreate(newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
    scaled, _, err := dc.scaleReplicaSetAndRecordEvent(newRS, *(deployment.Spec.Replicas), deployment)
    return scaled, err
}
```

### Recreate vs RollingUpdate 对比

| 维度 | Recreate | RollingUpdate |
|------|----------|---------------|
| 更新过程 | 先全停再全起 | 交替扩新缩旧 |
| 停机时间 | 有（旧 pod 停到新 pod 起） | 无（受 maxUnavailable 保护） |
| 超出期望 pod 数 | 不超出 | 最多多 maxSurge 个 |
| 适用场景 | 不能新旧版本共存（如 DB schema 变更） | 默认场景，保持可用性 |
| 实现复杂度 | 简单 | 复杂（多轮迭代） |
