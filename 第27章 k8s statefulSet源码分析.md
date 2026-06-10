# 第27章 k8s StatefulSet 源码分析

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 27 章 — StatefulSet 控制器源码分析
> **源码入口**: `pkg/controller/statefulset/stateful_set.go`

---

## 核心机制一览

1. **两个数组驱动所有 Pod 操作**：`updateStatefulSet` 维护两个切片：`replicas`（序号 0~N-1 范围内、应该存在的 Pod）和 `condemned`（序号 ≥ N、应该被删除的 Pod）。所有的新增、删除、滚动更新逻辑都围绕这两个数组展开，遍历方向不同：`replicas` 顺序遍历（正序创建），`condemned` 逆序遍历（逆序删除）。

2. **`monotonic` 模式（默认）**：每个 sync 循环只做一件事，严格保证有序性。新增时：发现 `replicas[i]` 尚未创建 → 创建并立即 `return`，等下一个 sync 循环再创建下一个。删除时：condemned 列表逆序删，等 Pod `Available` 之后才删下一个。如果任意 Pod 正在 Terminating 或 NotRunningAndReady，也立即 `return` 等待。这就是"有序部署和缩放"的源码实现。

3. **`syncStatefulSet` 与 `UpdateStatefulSet` 的分层**：`sync` → `syncStatefulSet` 只负责从 informer 拿对象、收集 pod，再调 `ssc.control.UpdateStatefulSet`（接口方法）。真正的业务逻辑在 `defaultStatefulSetControl.UpdateStatefulSet`（:74）→ `performUpdate`（:92）→ `updateStatefulSet`（:271），三层调用将接口定义与实现分离，方便测试替换。

4. **`ControllerRevision` 双版本管理**：每次 sync 都通过 `getStatefulSetRevisions`（:201）获取 `currentRevision`（当前稳定版本）和 `updateRevision`（目标版本）。Pod 的 `controller-revision-hash` label 记录它属于哪个 revision，滚动更新时通过对比 `getPodRevision(pod) == updateRevision.Name` 判断是否需要更新，**不是**对比 Pod spec，而是对比 revision 名称字符串。

5. **StatefulSet 的 PVC 生命周期独立于 Pod**：每个 Pod 拥有独立的 PVC（`www-web-0`、`www-web-1`...），删除 Pod 不删 PVC。回滚时 PV 里的数据无法回滚，生产环境需谨慎。非级联删除（`--cascade=orphan`）后重建 STS，原有 Pod 会被收养。

6. **动态 PV 通过 nfs-provisioner 实现**：StorageClass 的 Provisioner 字段注册一个外部 controller（如 `k8s-sigs.io/nfs-subdir-external-provisioner`），PVC 创建时该 controller 自动调 NFS API 创建子目录并绑定 PV，删除 PVC 时自动清理。

---

## 全章调用链总图

```
StatefulSetController
  │
  ├─── Pod Informer:         addPod / updatePod / deletePod
  └─── StatefulSet Informer: enqueueStatefulSet (add/update/delete)
  │
  ▼ worker → processNextWorkItem (stateful_set.go:401/385)
  │
  ▼ sync (stateful_set.go:407)
  │  ├─── setLister.StatefulSets(ns).Get(name)
  │  ├─── adoptOrphanRevisions (收养孤儿 ControllerRevision)
  │  ├─── getPodsForStatefulSet (ClaimPods 收养/释放孤儿 Pod)
  │  └─── ssc.control.UpdateStatefulSet(set, pods)
  │
  ▼ defaultStatefulSetControl.UpdateStatefulSet (stateful_set_control.go:74)
  │  └─── performUpdate (stateful_set_control.go:92)
  │         ├─── getStatefulSetRevisions → currentRevision, updateRevision
  │         ├─── updateStatefulSet(set, currentRevision, updateRevision, revisions, pods)
  │         ├─── truncateHistory (清理超出 revisionHistoryLimit 的历史)
  │         └─── updateStatefulSetStatus
  │
  ▼ updateStatefulSet (stateful_set_control.go:271)
     ├─── 构造 replicas[] + condemned[]
     ├─── 找 firstUnhealthyPod
     ├─── 处理新增：顺序遍历 replicas，isFailed → 删旧建新；!isCreated → CreateStatefulPod
     │      └─── monotonic: 每轮只创建一个，立即 return
     ├─── 处理删除：逆序遍历 condemned → DeleteStatefulPod
     │      └─── monotonic: 等 Available 后才删下一个
     └─── 处理滚动更新：倒序遍历 replicas，Revision != updateRevision → DeleteStatefulPod
            └─── 下一轮 sync 时重建新版本 Pod
```

---

## §01 StatefulSet 常见功能 — 动态 PV 准备

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| PVC 与 Pod 关系 | [stateful_set_utils.go](kubernetes/pkg/controller/statefulset/stateful_set_utils.go) | — |

### StatefulSet 适用场景

StatefulSet 管理**有状态应用**，保证以下特性：
- **稳定、唯一的网络标识符**：Pod 名称固定为 `<sts-name>-<ordinal>`，通过 Headless Service 可以用 `<pod-name>.<service-name>` DNS 名访问
- **稳定、持久的存储**：每个 Pod 绑定独立的 PVC，删除 Pod 不删 PVC
- **有序、优雅的部署和缩放**：创建顺序 0→N-1，删除逆序 N-1→0
- **有序、自动的滚动更新**：从序号最大的 Pod 开始逐个更新

### 动态 PV vs 静态 PV

静态 PV 需要管理员手动创建，数量受限；动态 PV 通过 **StorageClass** 自动触发 Provisioner 创建，解决了按需供应的问题。

Kubernetes 引入 StorageClass 后，PVC 声明 `storageClassName` 即可触发对应 Provisioner 自动创建 PV。

### 使用 nfs-subdir-external-provisioner 实现动态 PV

**步骤01 服务端安装 NFS**：
```bash
# 安装 nfs-utils
yum -y install nfs-utils
# 配置 /etc/exports
/data/nfs/k8s *(rw,sync,no_root_squash)
# 启动服务
systemctl start nfs-server
# 验证导出
showmount -e localhost
# 设置目录权限
mkdir -pv /data/nfs/k8s
chown -R nfsnobody.nfsnobody /data/nfs/k8s
```

**步骤02 客户端安装 NFS**：
```bash
yum -y install nfs-utils
# 测试挂载
mount -t nfs 172.20.70.205:/data/nfs/k8s /mnt
```

**步骤03 部署 nfs-client（nfs-subdir-external-provisioner）**：

从 GitHub clone `nfs-subdir-external-provisioner` 仓库后，需修改 `deploy/deployment.yaml` 中的 NFS Server 地址和路径，然后 apply：
```bash
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/deployment.yaml
```

**部署 StorageClass**：
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: managed-nfs-storage
provisioner: k8s-sigs.io/nfs-subdir-external-provisioner
parameters:
  pathPattern: "${.PVC.namespace}/${.PVC.annotations.nfs.io/storage-path}"
  onDelete: delete
```

验证动态 PV 工作流程：创建 PVC → Provisioner 自动在 NFS 目录下创建子目录并绑定 PV → Pod 挂载使用。删除 PVC 后，Provisioner 自动清理 NFS 子目录（日志可见 `delete` 操作）。

---

## §02 StatefulSet 常见功能 — 新增、删除、扩容

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Pod 顺序创建 | [stateful_set_control.go](kubernetes/pkg/controller/statefulset/stateful_set_control.go) | `updateStatefulSet:271` |

### StatefulSet、Pod、PVC、PV 关系图

```
StatefulSet web
  ├── Pod template
  └── volumeClaimTemplate
         │
         ├── Pod web-0  ──► PVC web-0  ──► PV
         ├── Pod web-1  ──► PVC web-1  ──► PV
         └── Pod web-2  ──► PVC web-2  ──► PV
```

StatefulSet 下每个 Pod 正常情况下都会关联一个 PV 对象。对 StatefulSet 对象回滚非常容易，但其使用的 PV 中保存的数据无法回滚，生产环境谨慎操作。

### 创建演示

StatefulSet YAML 示例（使用 managed-nfs-storage StorageClass）：
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web
spec:
  serviceName: "nginx"
  replicas: 2
  selector:
    matchLabels:
      app: nginx
  template:
    # ...
    spec:
      containers:
      - name: nginx
        image: nginx:1.7.9
        volumeMounts:
        - name: www
          mountPath: /usr/share/nginx/html
  volumeClaimTemplates:
  - metadata:
      name: www
    spec:
      storageClassName: managed-nfs-storage
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

apply 后观察：**Pod 按照 (0..N-1) 的序号顺序创建，且会等待前一个 Pod 变为 Running & Ready 后才会启动下一个 Pod。** 同时自动创建两个 PV 和 PVC，使用配置的 managed-nfs-storage StorageClass。

### 扩容

```bash
kubectl scale sts web --replicas=4
```

watch pod 变化：顺序创建，且上一个 Ready 之后才会创建下一个（web-2 Ready → web-3 开始创建）。

### 缩容

```bash
kubectl scale sts web --replicas=2
```

控制器按照 Pod 序号**引相反的顺序**每次删除一个 Pod，在删除下一个 Pod 前会等待上一个被完全删除（web-3 Terminating 完成 → web-2 Terminating）。

### 滚动更新

更新策略由 `spec.updateStrategy.type` 字段决定：`OnDelete` 或 `RollingUpdate`（默认）。

使用 RollingUpdate 时，所有 Pod 采用**与序号索引相反的顺序**进行更新。根据 `spec.updateStrategy.rollingUpdate.partition` 参数进行分段更新（序号 ≥ partition 的 Pod 才更新）。

```bash
kubectl patch statefulset web --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": "nginx:1.9.1"}]'
kubectl rollout status sts/web
```

### 回滚

```bash
kubectl rollout history statefulset web
kubectl get controllerrevision
kubectl rollout undo sts/web --to-revision=<N>
```

StatefulSet 的回滚**不是自动的**，回滚操作仅仅是进行了一次发布更新（更新 updateRevision 为历史版本的 template）。

### 删除

- **非级联删除**（`--cascade=orphan`）：只删 StatefulSet，Pod 还在运行；重新 apply 同一 STS 后会收养已有 Pod
- **级联删除**：`kubectl delete statefulset web` — 控制器先进行类似缩容的操作，Pod 按序号反向逐个终止；在终止一个 Pod 前，控制器会等待 Pod 后继者被完全终止

---

## §03 StatefulSetController 初始化

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 控制器构造 | [stateful_set.go](kubernetes/pkg/controller/statefulset/stateful_set.go) | `NewStatefulSetController:77` |
| Pod Informer 回调 | [stateful_set.go](kubernetes/pkg/controller/statefulset/stateful_set.go) | `addPod:161` / `updatePod:195` / `deletePod:242` |
| STS Informer 回调 | [stateful_set.go](kubernetes/pkg/controller/statefulset/stateful_set.go) | `enqueueStatefulSet:374` |

### 入口与依赖

`startStatefulSetController`（`cmd/kube-controller-manager/app/apps.go`）调用 `statefulset.NewStatefulSetController`，传入四个 Informer：
- `Pods` Informer
- `StatefulSets` Informer
- `PersistentVolumeClaims` Informer
- `ControllerRevisions` Informer

以及一个 `kubeClient`。

### `NewStatefulSetController` 核心结构

```go
// pkg/controller/statefulset/stateful_set.go:77
func NewStatefulSetController(...) *StatefulSetController {
    eventBroadcaster := record.NewBroadcaster()
    eventBroadcaster.StartStructuredLogging(0)
    eventBroadcaster.StartRecordingToSink(...)

    ssc := &StatefulSetController{
        kubeClient: kubeClient,
        control: NewDefaultStatefulSetControl(
            NewRealStatefulPodControl(
                kubeClient,
                setInformer.Lister(),
                podInformer.Lister(),
                pvcInformer.Lister(),
                recorder,
            ),
            NewRealStatefulSetStatusUpdater(kubeClient, setInformer.Lister()),
            history.NewHistory(kubeClient, revInformer.Lister()),
            recorder,
        ),
        pvcListerSynced: pvcInformer.Informer().HasSynced,
        queue:           workqueue.NewNamedRateLimitingQueue(...),
        podControl:      controller.RealPodControl{...},
        revListerSynced: revInformer.Informer().HasSynced,
    }
    // ...
}
```

- `control`（`StatefulSetControlInterface`）：核心接口，封装 `UpdateStatefulSet`、`ListRevisions` 等方法，实现类为 `defaultStatefulSetControl`
- `RealStatefulPodControl`：封装 `CreateStatefulPod`、`DeleteStatefulPod`、`UpdateStatefulPod`
- `history`：封装 `ControllerRevision` 的 CRUD

### Pod Informer 事件回调

**`addPod`（:161）**：
- 若 `DeletionTimestamp != nil` → 转 `deletePod`
- 若有 `ControllerRef` → 解析所属 STS，调 `enqueueStatefulSet`
- 孤儿 Pod → `getStatefulSetsForPod` 找匹配 STS，入队（尝试收养）

**`updatePod`（:195）**：
- `ResourceVersion` 不变 → 跳过（periodic resync）
- `DeletionTimestamp` 变化 → 转 `deletePod` 处理
- 主人 STS 发生变化（ControllerRef 变更）→ 同步旧 STS
- 标签变化 → 同步新 STS
- 若 `MinReadySeconds > 0`，`IsPodAvailable` 为 true → 使用 `AddAfter` 延迟重新入队（等满足 MinReadySeconds 后再检查 available 数量）

**`deletePod`（:242）**：从 tombstone 中恢复 pod，解析所属 STS，调 `enqueueStatefulSet`。

### StatefulSet Informer 事件回调

```go
setInformer.Informer().AddEventHandler(
    cache.ResourceEventHandlerFuncs{
        AddFunc:    ssc.enqueueStatefulSet,
        UpdateFunc: func(old, cur interface{}) {
            oldPS := old.(*apps.StatefulSet)
            curPS := cur.(*apps.StatefulSet)
            if oldPS.Status.Replicas != curPS.Status.Replicas {
                klog.V(4).Infof("Observed updated replica count...")
            }
            ssc.enqueueStatefulSet(cur)
        },
        DeleteFunc: ssc.enqueueStatefulSet,
    },
)
```

增删改回调都只做一件事：**`enqueueStatefulSet` 入队**。

### `Run` 入口

```go
// pkg/controller/statefulset/stateful_set.go:142
func (ssc *StatefulSetController) Run(workers int, stopCh <-chan struct{}) {
    // 等待 pod、sts、pvc、rev informer 全部同步
    if !cache.WaitForNamedCacheSync("stateful set", stopCh,
        ssc.podListerSynced, ssc.setListerSynced, ...) {
        return
    }
    for i := 0; i < workers; i++ {
        go wait.Until(ssc.worker, time.Second, stopCh)
    }
    <-stopCh
}
```

---

## §04 StatefulSetController sync 同步

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 同步入口 | [stateful_set.go](kubernetes/pkg/controller/statefulset/stateful_set.go) | `sync:407` |
| 业务入口 | [stateful_set_control.go](kubernetes/pkg/controller/statefulset/stateful_set_control.go) | `UpdateStatefulSet:74` / `performUpdate:92` |
| 版本管理 | [stateful_set_control.go](kubernetes/pkg/controller/statefulset/stateful_set_control.go) | `getStatefulSetRevisions:201` |
| 核心同步 | [stateful_set_control.go](kubernetes/pkg/controller/statefulset/stateful_set_control.go) | `updateStatefulSet:271` |

### `sync` 主逻辑

```go
// pkg/controller/statefulset/stateful_set.go:407
func (ssc *StatefulSetController) sync(key string) error {
    namespace, name, err := cache.SplitMetaNamespaceKey(key)

    // 从 informer 本地缓存获取 sts
    set, err := ssc.setLister.StatefulSets(namespace).Get(name)
    if errors.IsNotFound(err) {
        return nil
    }

    // 解析标签选择器
    selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)

    // 尝试领养孤儿 ControllerRevision（把 OwnerReferences 设置为这个 sts）
    if err := ssc.adoptOrphanRevisions(set); err != nil {
        return err
    }

    // 通过 selector 获取 sts 关联的 pod（ClaimPods：match/adopt/release 三流程）
    pods, err := ssc.getPodsForStatefulSet(set, selector)

    // 调用核心接口
    return ssc.syncStatefulSet(set, pods)
}
```

`syncStatefulSet` 简单地调 `ssc.control.UpdateStatefulSet(set, pods)`。

### `UpdateStatefulSet` — 接口层

```go
// pkg/controller/statefulset/stateful_set_control.go:74
func (ssc *defaultStatefulSetControl) UpdateStatefulSet(
    set *apps.StatefulSet, pods []*v1.Pod) error {
    // ...
    return ssc.performUpdate(ctx, set, pods, revisions)
}
```

### `performUpdate` — 版本管理

```go
// pkg/controller/statefulset/stateful_set_control.go:92
func (ssc *defaultStatefulSetControl) performUpdate(...) (
    *apps.ControllerRevision, *apps.ControllerRevision, *apps.StatefulSetStatus, error) {

    currentRevision, updateRevision, status, err :=
        ssc.updateStatefulSet(ctx, set, currentRevision, updateRevision, revisions, pods)

    // 清理超出 revisionHistoryLimit 的历史版本
    err = ssc.truncateHistory(set, pods, revisions, currentRevision, updateRevision)

    // 调用 updateStatefulSetStatus 写回 status
    return currentRevision, updateRevision, status, nil
}
```

**`getStatefulSetRevisions`（:201）**：
1. `ListRevisions` 获取所有 ControllerRevision
2. `SortControllerRevisions` 排序（先按 Revision 号升序，Revision 相同按 CreationTimestamp，再按 Name 排序）
3. 返回 `currentRevision`（当前稳定版本）和 `updateRevision`（目标版本）

**`SortControllerRevisions` 排序规则**：
```go
// revision 号小的排前面；revision 相同时，创建时间早的排前面（CreationTimestamp.After 比较）；
// 最后按 Name 字母序
func (br byRevision) Less(i, j int) bool {
    if br[i].Revision == br[j].Revision {
        if br[j].CreationTimestamp.Equal(&br[i].CreationTimestamp) {
            return br[i].Name < br[j].Name
        }
        return br[j].CreationTimestamp.After(br[i].CreationTimestamp.Time)
    }
    return br[i].Revision < br[j].Revision
}
```

### `updateStatefulSet` — 核心同步

#### 第一步：构造 replicas 和 condemned 两个数组

```go
// pkg/controller/statefulset/stateful_set_control.go:271
// replicaCount = set.Spec.Replicas
replicas := make([]*v1.Pod, replicaCount)
condemned := make([]*v1.Pod, 0, len(pods))

for i := range pods {
    ord := getOrdinal(pods[i])  // 从 Pod 名称解析序号
    if 0 <= ord && ord < replicaCount {
        replicas[ord] = pods[i]  // 序号在范围内 → 放 replicas
    } else if ord >= replicaCount {
        condemned = append(condemned, pods[i])  // 序号超出 → 放 condemned
    }
    // ord < 0 → 解析失败，忽略
}

// replicas 中空位（被删除的或从未创建的）用新 Pod 对象填充
for ord := 0; ord < replicaCount; ord++ {
    if replicas[ord] == nil {
        replicas[ord] = newVersionedStatefulSetPod(
            currentSet, updateSet, currentRevision.Name, updateRevision.Name, ord)
    }
}
```

两个数组的语义：
- `replicas[i]`：序号 i 的 Pod（已存在或待创建），序号 < `Spec.Replicas`
- `condemned`：序号 ≥ `Spec.Replicas` 的 Pod，需要逆序删除

#### 第二步：统计 unhealthy Pod

```go
// 找到第一个 unhealthy Pod（replicas 和 condemned 中都找）
for i := range replicas {
    if !isHealthy(replicas[i]) {
        unhealthy++
        if firstUnhealthyPod == nil {
            firstUnhealthyPod = replicas[i]
        }
    }
}
// condemned 中同理
```

若 `set.DeletionTimestamp != nil`（StatefulSet 正在被删除），只更新 status，不做 Pod 操作，直接 `return`。

#### 第三步：处理新增 Pod（顺序遍历 replicas）

```go
for i := range replicas {
    // 1. failed Pod → 删旧建新
    if isFailed(replicas[i]) {
        ssc.recorder.Eventf(set, v1.EventTypeWarning, "RecreatingFailedPod", ...)
        if err := ssc.podControl.DeleteStatefulPod(set, replicas[i]); err != nil {
            return &status, err
        }
        // 更新 revision 计数，用新对象替换
        replicas[i] = newVersionedStatefulSetPod(...)
    }
    // 2. 尚未创建 → 调 CreateStatefulPod
    if !isCreated(replicas[i]) {
        if err := ssc.podControl.CreateStatefulPod(set, replicas[i]); err != nil {
            return &status, err
        }
        status.Replicas++
        // 关键：monotonic 模式下创建一个立即返回，保证有序
        if monotonic {
            return &status, nil
        }
        continue
    }
    // 3. Pod 正在 Terminating → 等待
    if isTerminating(replicas[i]) && monotonic {
        klog.V(4).Infof("StatefulSet %s/%s is waiting for Pod %s to Terminate",...)
        return &status, nil
    }
    // 4. Pod 未 RunningAndReady → 等待
    if !isRunningAndReady(replicas[i]) && monotonic {
        klog.V(4).Infof("StatefulSet %s/%s is waiting for Pod %s to be Running and Ready",...)
        return &status, nil
    }
    // 5. Pod 存在且正常，用 DeepCopy 更新 Pod 状态字段
    replica := replicas[i].DeepCopy()
    if err := ssc.podControl.UpdateStatefulPod(updateSet, replica); err != nil {
        return &status, err
    }
}
```

#### 第四步：处理删除 Pod（逆序遍历 condemned）

```go
for target := len(condemned) - 1; target >= 0; target-- {
    if isTerminating(condemned[target]) {
        // monotonic 模式：等待 Terminating 完成后再删下一个
        if monotonic {
            return &status, nil
        }
        continue
    }
    // 检查是否需要等待（仅 monotonic 且当前不是第一个 unhealthy 的情况）
    if !isRunningAndAvailable(condemned[target], set.Spec.MinReadySeconds) && monotonic &&
        condemned[target] != firstUnhealthyPod {
        return &status, nil
    }
    // 执行删除
    if err := ssc.podControl.DeleteStatefulPod(set, condemned[target]); err != nil {
        return &status, err
    }
    if monotonic {
        return &status, nil
    }
}
```

#### 第五步：处理滚动更新

```go
// OnDelete 策略：短路返回（等用户手动删除）
if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
    return &status, nil
}

// RollingUpdate：倒序遍历 replicas，找需要更新的 Pod
updateMin := 0
if set.Spec.UpdateStrategy.RollingUpdate != nil {
    updateMin = int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
}
for target := len(replicas) - 1; target >= updateMin; target-- {
    // Pod 的 Revision != updateRevision → 需要更新
    if getPodRevision(replicas[target]) != updateRevision.Name &&
        !isTerminating(replicas[target]) {
        // 删除旧 Pod，下一次 sync 时 updateStatefulSet 会在 replicas[target] 为 nil 处
        // 创建新版本 Pod（通过 newVersionedStatefulSetPod 使用 updateRevision）
        if err := ssc.podControl.DeleteStatefulPod(set, replicas[target]); err != nil {
            return &status, err
        }
        status.UpdatedReplicas--
        if monotonic {
            return &status, nil  // 每轮只更新一个
        }
    }
}
```

**滚动更新的核心流程（monotonic 模式）**：
1. 第 N 次 sync：发现 `replicas[i].Revision != updateRevision` → 删旧 Pod → return
2. 第 N+1 次 sync：`replicas[i]` 为 nil → 创建新版 Pod（使用 updateRevision template）→ return
3. 第 N+2 次 sync：新 Pod Running & Ready → 继续处理下一个序号

**`Partition` 参数**：只更新序号 ≥ Partition 的 Pod，序号 < Partition 的保持旧版本，实现金丝雀发布。
