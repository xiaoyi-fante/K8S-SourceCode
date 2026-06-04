# 第17章 kubelet 的 GarbageCollection

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 17 章 — kubelet 的 GarbageCollection
> **源码入口**: `pkg/kubelet/images/image_gc_manager.go` / `pkg/kubelet/kuberuntime/kuberuntime_gc.go`

---

## 核心机制一览

1. **镜像 GC 双阈值触发策略**：只有磁盘使用率超过 `HighThresholdPercent` 才触发回收；回收目标是将使用率降到 `LowThresholdPercent`。按 LRU（最近最少使用）顺序删除，secondarySort 按 `firstDetected` 时间（同 lastUsed 时先删最早发现的）。

2. **imageRecords 缓存是关键状态**：`realImageGCManager` 维护一张 `imageRecords map[string]*imageRecord`，记录每个镜像的 `firstDetected`、`lastUsed`、`size`。`detectImages` 每次 GC 前刷新这张表，并从其中过滤出不在 `imagesInUse` 中的镜像作为驱逐候选。

3. **detectImages 三件事**：① 通过 gRPC `GetImageRef` 把 sandbox 镜像（pause）固定加入 `imagesInUse`；② 通过 gRPC `ListImages` + `GetPods` 构建 `imagesInUse`（当前所有容器正在使用的镜像 ID set）；③ 更新 `imageRecords` 缓存：新镜像记录 firstDetected、在用则更新 lastUsed、消失则删除记录。

4. **容器 GC 三级清理**：`GarbageCollect` 依次执行 ① `evictContainers`（按策略删除死容器）→ ② `evictSandboxes`（删除无容器的 sandbox）→ ③ `evictPodLogsDirectories`（删除 pod 日志目录）。

5. **evictContainers 两轮驱逐**：第一轮按 `MaxPerPodContainer`（每 pod 最多保留 N 个死容器，默认 1）用 `enforceMaxContainersPerEvictUnit` 裁剪；第二轮按 `MaxContainers`（全节点上限）将所有 evictUnit 拍平后再次用 `removeOldestN` 裁剪。

6. **容器保留旧实例的意义**：dead 容器保留数 > 0 是为了 debug——出错的容器如果立即删除，日志和现场就消失了。`maximum-dead-containers-per-container` 默认为 1，保留最近一次的失败现场。

---

## 全章调用链总图

```
kubelet.StartGarbageCollection (kubelet.go:1281)
  │
  ├─ 每 ImageGCPeriod(5min) 执行一次镜像 GC:
  │    imageManager.GarbageCollect (image_gc_manager.go:273)
  │      │
  │      ├─ statsProvider.ImageFsStats()    → 获取磁盘 capacity/available
  │      ├─ 计算 usagePercent，若 < HighThresholdPercent → 跳过
  │      └─ freeSpace(amountToFree, now)   (image_gc_manager.go:332)
  │           ├─ detectImages(freeTime)    (image_gc_manager.go:209)
  │           │    ├─ runtime.GetImageRef(sandboxImage) → 固定加入 imagesInUse
  │           │    ├─ runtime.ListImages() → 刷新 currentImages
  │           │    ├─ runtime.GetPods()    → 遍历容器收集 imagesInUse
  │           │    └─ 更新 imageRecords 缓存（新增/更新lastUsed/删除）
  │           ├─ 按 byLastUsedAndDetected 排序候选镜像（LRU 优先删）
  │           └─ runtime.RemoveImage(imageID) until spaceFreed >= bytesToFree
  │
  └─ 每 ContainerGCPeriod 执行一次容器 GC:
       containerGC.GarbageCollect (kuberuntime_gc.go:400)
         │
         ├─ evictContainers (kuberuntime_gc.go:223)
         │    ├─ evictableContainers()  → ListContainers(all) 过滤 running/minAge
         │    │    └─ 按 evictUnit{podUID, containerName} 分组
         │    ├─ 第一轮: enforceMaxContainersPerEvictUnit  (kuberuntime_gc.go:114)
         │    │    └─ 每个 evictUnit 超过 MaxPerPodContainer → removeOldestN
         │    └─ 第二轮: MaxContainers 全局上限
         │         └─ 拍平所有 evictUnit → removeOldestN (kuberuntime_gc.go:125)
         │              └─ unknown 状态容器先 killContainer 再 removeContainer
         │
         ├─ evictSandboxes (kuberuntime_gc.go:275)
         └─ evictPodLogsDirectories (kuberuntime_gc.go:329)
```

---

## §01 GarbageCollection 之镜像清理源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| GC 触发入口 | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `StartGarbageCollection:1281` |
| 镜像 GC 管理器构造 | [images/image_gc_manager.go](kubernetes/pkg/kubelet/images/image_gc_manager.go) | `NewImageGCManager:152` |
| GarbageCollect 主流程 | [images/image_gc_manager.go](kubernetes/pkg/kubelet/images/image_gc_manager.go) | `GarbageCollect:273` |
| 镜像在用检测 | [images/image_gc_manager.go](kubernetes/pkg/kubelet/images/image_gc_manager.go) | `detectImages:209` |
| 释放磁盘空间 | [images/image_gc_manager.go](kubernetes/pkg/kubelet/images/image_gc_manager.go) | `freeSpace:332` |

### 镜像回收策略

镜像 GC 只考虑两个阈值：
- `HighThresholdPercent`：磁盘使用率超过此值时触发回收
- `LowThresholdPercent`：回收目标，将使用率降至此值以下

回收的意思是：计算需要释放的大小（`amountToFree`），然后按 LRU 顺序遍历候选镜像逐个删除，直到释放量达到 `amountToFree`。

### realImageGCManager 结构

```go
// pkg/kubelet/images/image_gc_manager.go
type realImageGCManager struct {
    runtime        container.Runtime       // 用于 gRPC 删除镜像
    imageRecords   map[string]*imageRecord // 镜像 ID → 元数据缓存
    imageRecordsLock sync.Mutex
    policy         ImageGCPolicy           // High/LowThresholdPercent
    statsProvider  StatsProvider           // 提供磁盘使用统计
    recorder       record.EventRecorder
    nodeRef        *v1.ObjectReference
    initialized    bool
    imageCache     imageCache              // 最近一次 ListImages 结果缓存
    sandboxImage   string                  // 不能被回收的 pause 镜像，如 k8s.gcr.io/pause:3.6
}
```

`sandboxImage` 对应 `cmd/kubelet/app/options/container_runtime.go` 中定义的默认值（`k8s.gcr.io/pause:3.6`），pause 容器镜像始终不被 GC 删除——它是每个 Pod sandbox 的基础镜像。

### GC 触发：StartGarbageCollection

`StartGarbageCollection` 在 kubelet 启动时调用，内部用 `wait.Until` 定时循环：

```go
// pkg/kubelet/kubelet.go:1281
func (kl *Kubelet) StartGarbageCollection() {
    // 镜像 GC，每 ImageGCPeriod（5min）执行一次
    prevImageGCFailed := false
    go wait.Until(func() {
        if err := kl.imageManager.GarbageCollect(); err != nil {
            if prevImageGCFailed {
                // 连续失败才产生 Warning event（避免偶发报警）
                kl.recorder.Eventf(kl.nodeRef, v1.EventTypeWarning, events.ImageGCFailed, ...)
            } else {
                klog.ErrorS(err, "Image garbage collection failed once. Stats initialization may not have completed yet")
            }
            prevImageGCFailed = true
        } else {
            prevImageGCFailed = false
        }
    }, ImageGCPeriod, wait.NeverStop)

    // 容器 GC，每 ContainerGCPeriod 执行一次（下节）
    go wait.Until(func() {
        kl.containerGC.GarbageCollect()
    }, ContainerGCPeriod, wait.NeverStop)
}
```

连续失败才产生 event 的设计：kubelet 刚启动时 statsProvider 可能尚未初始化完成，首次 GC 失败是正常的，避免产生误报 Warning。

### GarbageCollect：阈值判断与 freeSpace

```go
// pkg/kubelet/images/image_gc_manager.go:273
func (im *realImageGCManager) GarbageCollect() error {
    // 1. 获取磁盘统计
    fsStats, err := im.statsProvider.ImageFsStats()
    capacity  := int64(*fsStats.CapacityBytes)
    available := int64(*fsStats.AvailableBytes)

    // 2. 计算使用率，判断是否超过高阈值
    usagePercent := 100 - int(available*100/capacity)
    if usagePercent >= im.policy.HighThresholdPercent {
        // 需要释放的量：降到低阈值所需空间
        amountToFree := capacity*int64(100-im.policy.LowThresholdPercent)/100 - available
        freed, err := im.freeSpace(amountToFree, time.Now())
        if freed < amountToFree {
            // 产生 FreeDiskSpaceFailed event
        }
    }
}
```

### detectImages：构建 imagesInUse 并刷新缓存

`detectImages` 是 `freeSpace` 的第一步，负责确定哪些镜像正在被使用：

```go
// pkg/kubelet/images/image_gc_manager.go:209
func (im *realImageGCManager) detectImages(detectTime time.Time) (sets.String, error) {
    imagesInUse := sets.NewString()

    // Step 1: sandbox 镜像（pause）永远不被回收
    imageRef, _ := im.runtime.GetImageRef(container.ImageSpec{Image: im.sandboxImage})
    if imageRef != "" {
        imagesInUse.Insert(imageRef)
    }

    // Step 2: 通过 ListImages 获取机器上所有镜像
    images, _ := im.runtime.ListImages()
    // 通过 GetPods 获取所有 pod，遍历容器收集正在使用的镜像 ID
    pods, _ := im.runtime.GetPods(true)
    for _, pod := range pods {
        for _, container := range pod.Containers {
            imagesInUse.Insert(container.ImageID)
        }
    }

    // Step 3: 遍历本地镜像列表，更新 imageRecords 缓存
    currentImages := sets.NewString()
    for _, image := range images {
        currentImages.Insert(image.ID)
        if _, ok := im.imageRecords[image.ID]; !ok {
            // 新镜像：记录 firstDetected 时间
            im.imageRecords[image.ID] = &imageRecord{firstDetected: detectTime}
        }
        if isImageUsed(image.ID, imagesInUse) {
            // 正在使用：更新 lastUsed 为当前时间
            im.imageRecords[image.ID].lastUsed = detectTime
        }
        im.imageRecords[image.ID].size = image.Size
    }
    // 从缓存中删除已不存在的镜像记录
    for image := range im.imageRecords {
        if !currentImages.Has(image) {
            delete(im.imageRecords, image)
        }
    }
    return imagesInUse, nil
}
```

### freeSpace：LRU 排序后逐个删除

```go
// pkg/kubelet/images/image_gc_manager.go:332
func (im *realImageGCManager) freeSpace(bytesToFree int64, freeTime time.Time) (int64, error) {
    // 1. 刷新 imagesInUse
    imagesInUse, _ := im.detectImages(freeTime)

    // 2. 构建驱逐候选列表（排除 imagesInUse）
    images := []evictionInfo{}
    for image, record := range im.imageRecords {
        if isImageUsed(image, imagesInUse) {
            continue  // 正在使用，跳过
        }
        if freeTime.Sub(record.firstDetected) < im.policy.MinAge {
            continue  // 太新，未满 minAge
        }
        images = append(images, evictionInfo{id: image, imageRecord: *record})
    }

    // 3. LRU 排序：lastUsed 最旧的优先删，lastUsed 相同时 firstDetected 最早的优先
    sort.Sort(byLastUsedAndDetected(images))

    // 4. 逐个删除直到 spaceFreed >= bytesToFree
    var spaceFreed int64
    for _, image := range images {
        // 通过 gRPC 删除镜像
        _ = im.runtime.RemoveImage(container.ImageSpec{Image: image.id})
        delete(im.imageRecords, image.id)
        spaceFreed += image.size
        if spaceFreed >= bytesToFree {
            break
        }
    }
    return spaceFreed, nil
}
```

`byLastUsedAndDetected` 排序规则：先按 `lastUsed` 升序（最旧的排最前），若 `lastUsed` 相等（如都是 `freeTime.Equal(firstDetected)`，意味着镜像从未被使用）则按 `firstDetected` 升序。这确保从未使用过的镜像中，最早发现的会最先被清理。

---

## §02 GarbageCollection 之容器清理源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| containerGC 构造 | [container/container_gc.go](kubernetes/pkg/kubelet/container/container_gc.go) | `NewContainerGC` |
| GarbageCollect 主流程 | [kuberuntime/kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `GarbageCollect:400` |
| 可驱逐容器列表 | [kuberuntime/kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `evictableContainers:186` |
| 每 pod 上限驱逐 | [kuberuntime/kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `enforceMaxContainersPerEvictUnit:114` |
| 删除最旧 N 个 | [kuberuntime/kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `removeOldestN:125` |
| 驱逐容器主逻辑 | [kuberuntime/kuberuntime_gc.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_gc.go) | `evictContainers:223` |

### 用户配置的容器 GC 策略

| 参数 | 含义 | 默认值 |
|------|------|--------|
| `minimum-container-ttl-duration` | 容器结束后至少存活多久才可被 GC | 0（立即可 GC） |
| `maximum-dead-containers-per-container` | 每个容器允许保留的死亡实例最大数 | 1 |
| `maximum-dead-containers` | 整个节点死亡容器总上限 | -1（不限） |

保留旧实例的意义：出错容器被立即删除则日志和现场消失无法 debug。`maximum-dead-containers-per-container=1` 保留最近一次失败现场；设为 0 则关闭保留（不建议）。

### realContainerGC 结构

```go
// pkg/kubelet/container/container_gc.go
type realContainerGC struct {
    runtime             Runtime              // gRPC remove 容器
    policy              GCPolicy             // MinAge/MaxPerPodContainer/MaxContainers
    sourcesReadyProvider SourcesReadyProvider // 判断 all sources 是否就绪
}
```

`SourcesReadyProvider` 的作用：只有所有来源（apiserver、file、http）都就绪后，GC 才删除属于已删除 pod 的容器——避免 kubelet 刚启动尚未同步完 pod 列表时就误删容器。

### GarbageCollect：三步清理

```go
// pkg/kubelet/kuberuntime/kuberuntime_gc.go:400
func (cgc *containerGC) GarbageCollect(gcPolicy kubecontainer.GCPolicy,
    allSourcesReady bool, evictTerminatedPods bool) error {
    errors := []error{}

    // 第一步：驱逐可驱逐的容器（dead 容器）
    if err := cgc.evictContainers(gcPolicy, allSourcesReady, evictTerminatedPods); err != nil {
        errors = append(errors, err)
    }
    // 第二步：驱逐无容器的 sandbox
    if err := cgc.evictSandboxes(evictTerminatedPods); err != nil {
        errors = append(errors, err)
    }
    // 第三步：驱逐 pod 日志目录
    if err := cgc.evictPodLogsDirectories(allSourcesReady); err != nil {
        errors = append(errors, err)
    }
    return utilerrors.NewAggregate(errors)
}
```

### evictableContainers：构建可驱逐容器列表

```go
// pkg/kubelet/kuberuntime/kuberuntime_gc.go:186
func (cgc *containerGC) evictableContainers(minAge time.Duration) (containersByEvictUnit, error) {
    // 通过 gRPC ListContainers 获取所有容器（allContainers=true，包含 exited）
    containers, _ := cgc.manager.getKubeletContainers(true)

    evictUnits := containersByEvictUnit{}
    for _, container := range containers {
        // 过滤掉 running 容器
        if container.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
            continue
        }
        // 过滤掉 age < minAge 的容器（太新，还不到可 GC 的时间）
        createdAt := time.Unix(0, container.CreatedAt)
        if newestGCTime.Before(createdAt) {
            continue
        }
        // 按 evictUnit{podUID, containerName} 分组
        labeledInfo := getContainerInfoFromLabels(container.Labels)
        key := evictUnit{uid: labeledInfo.PodUID, name: containerInfo.name}
        evictUnits[key] = append(evictUnits[key], containerGCInfo{
            id:         container.Id,
            name:       container.Metadata.Name,
            createTime: createdAt,
            unknown:    container.State == runtimeapi.ContainerState_CONTAINER_UNKNOWN,
        })
    }
    return evictUnits, nil
}
```

**evictUnit 的设计**：以 `{podUID, containerName}` 为键分组，而不是以容器 ID 为键。同一个 pod 中同名容器（因重启产生多个实例）归为一组，便于按"每 pod 每容器名保留 N 个"的语义执行清理。

### evictContainers：两轮驱逐

```go
// pkg/kubelet/kuberuntime/kuberuntime_gc.go:223
func (cgc *containerGC) evictContainers(...) error {
    // 1. 获取可驱逐容器列表，按 evictUnit 分组
    evictUnits, _ := cgc.evictableContainers(gcPolicy.MinAge)

    // 2. 若 allSourcesReady，删除已删除/terminated pod 的全部容器
    if allSourcesReady {
        for key, unit := range evictUnits {
            if cgc.podStateProvider.ShouldPodContentBeRemoved(key.uid) ||
               (evictNonDeletedPods && ...) {
                cgc.removeOldestN(unit, len(unit))  // 全删
                delete(evictUnits, key)
            }
        }
    }

    // 3. 第一轮：按 MaxPerPodContainer 限制每个 evictUnit 的数量
    if gcPolicy.MaxPerPodContainer >= 0 {
        cgc.enforceMaxContainersPerEvictUnit(evictUnits, gcPolicy.MaxPerPodContainer)
    }

    // 4. 第二轮：按 MaxContainers 限制整个节点的总数
    if gcPolicy.MaxContainers >= 0 && evictUnits.NumContainers() > gcPolicy.MaxContainers {
        // 计算每个 evictUnit 应保留的平均数
        numContainersPerEvictUnit := gcPolicy.MaxContainers / evictUnits.NumEvictUnits()
        if numContainersPerEvictUnit < 1 {
            numContainersPerEvictUnit = 1
        }
        cgc.enforceMaxContainersPerEvictUnit(evictUnits, numContainersPerEvictUnit)

        // 如果仍然超过上限，将所有 evictUnit 拍平后按创建时间删最旧的
        if evictUnits.NumContainers() > gcPolicy.MaxContainers {
            flattened := []containerGCInfo{}
            for _, unit := range evictUnits {
                flattened = append(flattened, unit...)
            }
            sort.Sort(byCreated(flattened))
            cgc.removeOldestN(flattened, evictUnits.NumContainers()-gcPolicy.MaxContainers)
        }
    }
}
```

两轮驱逐的逻辑：第一轮保证每个容器名维度的上限（MaxPerPodContainer），第二轮保证节点总量上限（MaxContainers）。两轮都通过 `removeOldestN` 执行实际删除，unknown 状态的容器先 kill 再 remove。
