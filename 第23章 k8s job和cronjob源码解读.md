# 第23章 k8s job 和 cronjob 源码解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 23 章 — k8s job 和 cronjob 源码解读
> **源码入口**: `pkg/controller/job/job_controller.go`, `pkg/controller/cronjob/cronjob_controllerv2.go`

## 核心机制一览

1. **Job 的本质是带完成语义的 Pod 管理器**：Job controller 通过 `Completions`（期望成功次数）和 `Parallelism`（并发度）控制 Pod 的创建与删除。`syncJob` 是核心同步函数，它读取当前 active/succeeded/failed Pod 数，判断 Job 是否需要终止、是否需要同步，最终调用 `manageJob` 调整 Pod 数量。

2. **expectations 机制防止 watch 延迟导致的重复创建**：Job controller 在本地维护 `expectations` 缓存（add 和 del 两个计数器），每次 create/delete Pod 时预先记账，informer 回调 addPod/deletePod 时抵消。若两个计数器都为 0 才说明期望已满足，`jobNeedsSync` 返回 false，跳过不必要的 manageJob 调用。

3. **manageJob 的两个核心动作**：① **慢启动创建** pod — diff 从 1 开始指数增长（1/2/4/8...），与 ReplicaSet 相同，防止大量并发 pod 出现相同错误；② **限速删除** — `activePodsForRemoval` 选出要删除的 pod，数量受 `maxPodCreateDeletePerSync`（默认 500）限制，同一个 syncLoop 内只做删除或只做创建，不同时进行。

4. **CronJobControllerV1 vs V2 的本质差异**：V1 没有 informer 缓存、没有 workqueue，直接每 10 秒轮询 apiserver（`syncAll`）；V2 使用延迟队列和 informer，与 Job controller 架构一致，是 v1.21 的默认实现（feature gate `CronJobControllerV2`）。

5. **syncCronJob 的调度时间判断**：`getNextScheduleTime` 计算下次调度时间，起点是 `LastScheduleTime`（若无则用 CronJob 创建时间），用 `StartingDeadlineSeconds` 限制回溯窗口。`scheduledTime == nil` 说明队列已满（重启场景），直接返回等待下次调度。

6. **CronJob 并发策略三选一**：`Forbid`（同时有 active job 则跳过，产生 `JobAlreadyActive` event）、`Replace`（停止旧 job 启动新 job）、`Allow`（不限制，可同时运行多个 job）。底层创建动作统一调 `jobControl.CreateJob`。

---

## 全章调用链总图

```
kube-controller-manager startJobController
  │
  ▼ NewController(podInformer, jobInformer, kubeClient) — job_controller.go:99
  │   ├─ podInformer.AddEventHandler → addPod/updatePod/deletePod
  │   ├─ jobInformer.AddEventHandler → updateJob
  │   └─ expectations = controller.NewControllerExpectations()
  │
  ▼ Run(workers, stopCh) — job_controller.go:146
  │   └─ for i workers: go worker() → processNextWorkItem() → syncJob()
  │
  ▼ syncJob(key) — job_controller.go:443
  │   ├─ getPodsForJob (selector 过滤 + 认领孤儿 pod)
  │   ├─ getStatus → succeeded, failed
  │   ├─ pastBackoffLimitOnFailure? → deleteJobPods (删除所有 active pods)
  │   ├─ pastActiveDeadline? → 标记 JobFailed
  │   ├─ jobNeedsSync (expectations.SatisfiedExpectations) → manageJob
  │   │     └─ manageJob(job, activePods, succeeded, allPods) — job_controller.go:747
  │   │           ├─ jobSuspended → deleteJobPods
  │   │           ├─ active > wantActive → activePodsForRemoval → deleteJobPods
  │   │           └─ active < wantActive → 慢启动 for batchSize:= 1,2,4...
  │   │                 └─ podControl.CreatePodsWithGenerateName (goroutine)
  │   └─ status 变化 → updateJobStatus → patch apiserver
  │
  ▼ expectations 机制
      addPod     → expectations.CreationObserved(jobKey)   (add -1)
      deletePod  → expectations.DeletionObserved(jobKey)   (del -1)
      manageJob  → ExpectCreations / ExpectDeletions       (设置初值)

kube-controller-manager startCronJobControllerV2
  │
  ▼ NewControllerV2(jobInformer, cronJobsInformer, kubeClient) — cronjob_controllerv2.go:73
  │   ├─ job 回调: addJob/updateJob/deleteJob (解析 ownerRef → enqueueController)
  │   ├─ cronjob 回调: AddFunc/DeleteFunc → enqueueController
  │   │                UpdateFunc → updateCronJob (Schedule 变化 → enqueueControllerAfter)
  │   └─ queue = workqueue.NewNamedRateLimitingQueue(...)
  │
  ▼ Run → for i workers: go worker() → processNextWorkItem() → sync()
  │
  ▼ sync(cronJobKey) — cronjob_controllerv2.go:163
  │   ├─ cronJobLister.CronJobs(ns).Get(name)
  │   ├─ getJobsToBeReconciled → label selector 过滤有 ownerRef 的 job
  │   └─ syncCronJob(cj, js) — cronjob_controllerv2.go:399
  │         ├─ 遍历 js (childrenJobs map)
  │         │   ├─ 不在 active 且未完成 → UnexpectedJob event
  │         │   ├─ 在 active 且已完成 → deleteFromActiveList + SawCompletedJob event
  │         │   └─ IsJobFinished → 更新 LastSuccessfulTime
  │         ├─ 清理 active 中已不存在的 job (直接 GET apiserver，避免 watch 延迟)
  │         ├─ cronJobControl.UpdateStatus(cj) → patch apiserver
  │         ├─ cj.DeletionTimestamp != nil → 返回
  │         ├─ cj.Spec.Suspend → 返回
  │         ├─ getNextScheduleTime → scheduledTime
  │         │   ├─ earliestTime = LastScheduleTime 或 CreationTimestamp
  │         │   ├─ StartingDeadlineSeconds 限制回溯窗口
  │         │   ├─ missedRuns > 100 → TooManyMissedTimes event
  │         │   └─ scheduledTime == nil → 等待下次
  │         ├─ 判断此次是否已调度 (isJobInActiveList + LastScheduleTime == scheduledTime)
  │         ├─ ConcurrencyPolicy == Forbid + active > 0 → JobAlreadyActive event + 返回
  │         ├─ ConcurrencyPolicy == Replace → 删除旧 active jobs
  │         └─ jobControl.CreateJob(cj.Namespace, jobReq) → 新 Job
  │               └─ 更新 Status.Active / LastScheduleTime → UpdateStatus
  │
  └─ cleanupFinishedJobs → removeOldestJobs (按 SuccessfulJobsHistoryLimit / FailedJobsHistoryLimit)
```

---

## §01. Job 的基本功能

Job 是 Kubernetes 中用于管理**一次性任务**的工作负载资源。与 Deployment/ReplicaSet 管理常驻进程不同，Job 管理的 Pod 是为了完成某个任务后正常退出。

**Job 的核心字段**：

| 字段 | 含义 |
|------|------|
| `spec.completions` | 期望成功完成的 Pod 数（nil 表示任意一个成功即可） |
| `spec.parallelism` | 最大并发运行的 Pod 数 |
| `spec.backoffLimit` | 失败重试次数上限（默认 6） |
| `spec.activeDeadlineSeconds` | Job 的最大运行时长，超时后强制终止 |
| `spec.suspend` | 挂起 Job（不创建新 Pod，删除现有 active Pod） |
| `spec.ttlSecondsAfterFinished` | 完成后自动清理等待时间 |

**Job 的三种完成模式**（`completionMode`）：
- `NonIndexed`（默认）：任意一个 Pod 成功后 succeeded 计数 +1，直到达到 Completions
- `Indexed`：每个 Pod 携带唯一的 completionIndex，每个 index 只需一个 Pod 成功

**Job 状态字段**：`status.active`、`status.succeeded`、`status.failed`、`status.conditions`（JobComplete/JobFailed）。

---

## §02. Job controller 源码解析之初始化工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Job controller 初始化 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `NewController:99` |
| 启动 workers | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `Run:146` |
| pod 事件回调 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `addPod:204` |
| pod 更新回调 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `updatePod:240` |
| pod 删除回调 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `deletePod:292` |
| 工作循环 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `processNextWorkItem:389` |

### NewController 初始化

```go
// pkg/controller/job/job_controller.go:99
func NewController(podInformer coreinformers.PodInformer, jobInformer batchinformers.JobInformer, kubeClient clientset.Interface) *Controller {
    jm := &Controller{
        kubeClient:    kubeClient,
        podControl:    controller.RealPodControl{KubeClient: kubeClient, Recorder: eventBroadcaster.NewRecorder(...)},
        expectations:  controller.NewControllerExpectations(),
        queue:         workqueue.NewNamedRateLimitingQueue(workqueue.NewItemExponentialFailureRateLimiter(DefaultJobBackOff, MaxJobBackOff), "job"),
        workerLockMap: make(map[string]*sync.Mutex),
    }
    jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    jm.addJob,     // 新 job 直接入队
        UpdateFunc: jm.updateJob,  // 根据期望值决定是否入队
        DeleteFunc: jm.deleteJob,
    })
    podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    jm.addPod,
        UpdateFunc: jm.updatePod,
        DeleteFunc: jm.deletePod,
    })
    // ...
    return jm
}
```

### addPod/deletePod 与 expectations 的联动

**addPod**（job_controller.go:204）：
1. 若 `job.DeletionTimestamp` 存在，说明 job 正在删除，调 `jm.deleteJob(job)` 并返回
2. 获取 pod 的 ownerRef 中的 controllerRef（job 的名字），若无则忽略
3. 调 `jm.expectations.CreationObserved(jobKey)` — 将 expectations 中的 add 计数 -1（记账：这个 pod 已经被 informer 看到了）
4. 入队 `jm.enqueueController(job)`

**deletePod**（job_controller.go:292）：类似地，调 `jm.expectations.DeletionObserved(jobKey)`，将 del 计数 -1。

**正常情况**：addPod 和 deletePod 回调正常执行，add 和 del 最终都为 0，说明期望已满足，无需重试。若不满足则说明有失败的操作，等待 expectations 超时（5分钟）后再次同步。

### canAdoptFunc 与 releasePod

`syncJob` 中为 pod 认领（adopt）时会调用 `canAdoptFunc`：从 apiserver 中实时获取最新 job 信息（不走缓存），若 job 的 `DeletionTimestamp` 不为空，说明 job 已被删除，不能认领孤儿 pod。

`releasePod`（释放 pod 归属）：通过 patch 清空 pod 的 ownerReference，返回错误时记录 event。

---

## §03. Job controller 源码解析之 syncJob 工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 核心同步函数 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `syncJob:443` |
| pod 状态统计 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `getStatus:732` |
| 超出 BackoffLimit | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `pastBackoffLimitOnFailure:682` |
| 超出 ActiveDeadline | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `pastActiveDeadline:709` |
| 删除 active pods | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `deleteJobPods:654` |

### syncJob 整体流程

```
syncJob(key)
  │
  ├─ 从 lister 获取 job 对象
  ├─ getPodsForJob — 通过 selector 获取关联 pod，采用 adopt/release 模式处理孤儿 pod
  ├─ getStatus → activePods, succeeded, failed
  │
  ├─ ① 判断是否超出 BackoffLimit（失败重试次数）
  │     pastBackoffLimitOnFailure = failed > job.Spec.BackoffLimit &&
  │                                 (active > 0 || p.DeletionTimestamp == nil)
  │     → finishedCondition = JobFailed
  │
  ├─ ② 判断是否超出 ActiveDeadlineSeconds
  │     job.Status.StartTime + ActiveDeadlineSeconds <= now
  │     → finishedCondition = JobFailed（"DeadlineExceeded"）
  │
  ├─ ③ job 处于 failed 状态 → deleteJobPods 并发删除所有 active pods
  │
  ├─ ④ 若 job 未 failed：判断 jobNeedsSync
  │     jobNeedsSync = jm.expectations.SatisfiedExpectations(key)
  │     → 调用 manageJob
  │
  ├─ ⑤ 判断 pod 完成情况（complete）
  │     Completions == nil: active == 0 且至少一个成功
  │     Completions != nil: active == 0 且 succeeded >= Completions
  │     → finishedCondition = JobComplete
  │
  ├─ ⑥ 如果 pod succeeded 大于 Job 期望中 succeeded 的数量则设置 forget = true
  │     （避免 job backoff 时被加锁）
  │
  └─ ⑦ 如果 job status 变化 → updateStatusHandler → patch apiserver
```

### getStatus 函数

```go
// pkg/controller/job/job_controller.go:732
func getStatus(job *batch.Job, pods []*v1.Pod) (succeeded, failed int32) {
    // ...
    succeeded = int32(countValidPodsWithFilter(job, pods, func(p *v1.Pod) bool {
        return p.Status.Phase == v1.PodSucceeded
    }))
    failed = int32(countValidPodsWithFilter(job, pods, func(p *v1.Pod) bool {
        return p.Status.Phase == v1.PodFailed || isPodFinishedAfterFinalizer(p)
    }))
    return succeeded, failed
}
```

**getStatus 特殊逻辑**：当启用 finalizer 时，统计 failed pod 时会包含 `isPodFinishedAfterFinalizer`（即已经有 `batch.kubernetes.io/job-tracking` finalizer 且状态完成的 pod），避免在 finalizer 机制下漏计孤儿 pod 的失败次数。

### deleteActivePods 并发删除

```go
// pkg/controller/job/job_controller.go:654
func (jm *Controller) deleteJobPods(job *batch.Job, jobKey string, pods []*v1.Pod) (int32, error) {
    errCh := make(chan error, len(pods))
    wg.Add(len(pods))
    for i := range pods {
        go func(pod *v1.Pod) {
            defer wg.Done()
            if err := jm.podControl.DeletePod(job.Namespace, pod.Name, job); err != nil {
                atomic.AddInt32(&successfulDeletes, -1)
                errCh <- err
            }
        }(pods[i])
    }
    wg.Wait()
    return successfulDeletes, errorFromChannel(errCh)
}
```

---

## §04. Job controller 源码解析之 manageJob 工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| manageJob 主函数 | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `manageJob:747` |
| 选出待删除 pod | [job_controller.go](kubernetes/pkg/controller/job/job_controller.go) | `activePodsForRemoval:898` |

### 接上回：jobNeedsSync 的逻辑

`jobNeedsSync` 的判断依赖 `expectations.SatisfiedExpectations(key)`。

**expectations 机制分析**：
- 控制器除了有 informer 的缓存外，还有一个本地缓存叫 `expectations`
- `expectations` 记录控制器所有对象需要 add/del 的 pod 数量
- 若两者都为 0，说明控制器所期望的 pod 数已满足；不满足说明上次在 syncLoop 中有失败的操作，需等待 expectations 过期后再同步
- `SatisfiedExpectations` 判断方式：① key 在 ControllerExpectations 中不存在（全新控制器，还没设置期望）② `exp.Fulfilled()`（add 和 del 都 <= 0）③ `exp.isExpired()`（超过 5 分钟，强制重试）

**expectations 的 add/del 何时被调用**：
- `addPod` 中调用 `jm.expectations.CreationObserved(jobKey)`（add -1）
- `deletePod` 中调用 `jm.expectations.DeletionObserved(jobKey)`（del -1）
- `manageJob` 中调用 `ExpectCreations(jobKey, diff)` / `ExpectDeletions(jobKey, len(podsToDelete))`（设置初值）

**add 和 del 的初值从哪里设置**：`manageJob` 在创建/删除 pod 前先更新 expectations 缓存中的 del 个数，然后在 go func 匿名函数中执行实际操作。

### 正式分析 manageJob

```go
// pkg/controller/job/job_controller.go:747
func (jm *Controller) manageJob(job *batch.Job, activePods []*v1.Pod, succeeded int32, allPods []*v1.Pod) (int32, error) {
    active := int32(len(activePods))
    parallelism := *job.Spec.Parallelism
    jobKey, _ := controller.KeyFunc(job)
    // ...
```

**处理 job 挂起**：如果 `job.Spec.Suspend == true`，删除所有 active pods，更新 expectations del 个数。

**计算 wantActive**：
- `Completions == nil`：`wantActive` 等于 `parallelism`（持续运行直到第一个 pod 成功）
- `Completions != nil`：`wantActive = min(Completions - succeeded, parallelism)`

**删除多余 pod**（active > wantActive）：

```
rmAtLeast := active - wantActive
podsToDelete := activePodsForRemoval(job, activePods, int(rmAtLeast))
// 限速：最多删除 maxPodCreateDeletePerSync 个
if len(podsToDelete) > maxPodCreateDeletePerSync {
    podsToDelete = podsToDelete[:maxPodCreateDeletePerSync]
}
jm.expectations.ExpectDeletions(jobKey, len(podsToDelete))
removed, err = jm.deleteJobPods(job, jobKey, podsToDelete)
// 同一 syncLoop 内：有删就不创建（return immediately）
```

**慢启动创建需要的 pod**（active < wantActive）：

```
diff := wantActive - active
if diff > int32(maxPodCreateDeletePerSync) {
    diff = int32(maxPodCreateDeletePerSync)
}
jm.expectations.ExpectCreations(jobKey, int(diff))
// 慢启动：批次大小 1, 2, 4, 8...
for batchSize := int32(integer.IntMin(int(diff), controller.SlowStartInitialBatchSize)); diff > 0; {
    diff -= batchSize
    // ...
}
// 创建动作在 go func 匿名函数中
// 调用 jm.podControl.CreatePodsWithGenerateName → apiserver → kubelet
```

慢启动目的是防止大量创建 pod 出现相同的错误，批次大小呈指数增长（与 ReplicaSet 一致）。

若某批次创建失败（`errorCount < len(errCh)`）且有剩余（`skippedPods > 0`），将 `skippedPods` 记录在 expectations 中，下次 controller 重新处理。

---

## §05. CronJobController 同步主流程源码解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| V1 初始化 | [cronjob_controller.go](kubernetes/pkg/controller/cronjob/cronjob_controller.go) | `NewController:70` |
| V1 Run + syncAll | [cronjob_controller.go](kubernetes/pkg/controller/cronjob/cronjob_controller.go) | `Run:93` / `syncAll:103` |
| V2 初始化 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `NewControllerV2:73` |
| V2 Run + worker | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `Run:121` |
| V2 sync 入口 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `sync:163` |
| V2 获取关联 jobs | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `getJobsToBeReconciled:226` |
| V2 job 回调 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `addJob:251` / `updateJob:275` |
| V2 cronjob 更新回调 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `updateCronJob:358` |

### CronJobControllerV1 vs V2

| 维度 | V1 | V2 |
|------|----|----|
| 架构 | 无 informer，直接轮询 apiserver | informer 缓存 + workqueue |
| 触发方式 | `go wait.Until(syncAll, 10s, stopCh)` | event 驱动 + 延迟队列 |
| workqueue | 无 | 有（`NewNamedRateLimitingQueue`） |
| 入口 | `syncAll` | `worker → processNextWorkItem → sync` |
| 默认启用 | v1.21 之前 | v1.21 默认（feature gate `CronJobControllerV2`） |

### CronJobControllerV2 的 NewControllerV2

构造 `ControllerV2` 的关键字段：

```go
jm := &ControllerV2{
    queue:              workqueue.NewNamedRateLimitingQueue(...),
    recorder:           eventBroadcaster.NewRecorder(...),
    jobControl:         realJobControl{KubeClient: kubeClient},      // 操作 job 增删的接口
    cronJobControl:     &realCJControl{KubeClient: kubeClient},      // 更新 CronJob 状态的接口
    jobLister:          jobInformer.Lister(),
    cronJobLister:      cronJobsInformer.Lister(),
    jobListerSynced:    jobInformer.Informer().HasSynced,
    cronJobListerSynced: cronJobsInformer.Informer().HasSynced,
    now:                time.Now,
}
```

**与 V1 相比最显著的不同就是有了 workqueue**。

### job 事件回调（V2）

**addJob**：若 job 的 `DeletionTimestamp` 不为空则删除 job；然后获取 ownerRef 中的 controllerRef，解析出归属的 cronJob，调 `jm.enqueueController(cronJob)` 入队。

**updateJob**：获取新老 job 对象，若 `ResourceVersion` 相同（同一个对象）则跳过；否则解析新 job 的 controllerRef，判断是否有变化，然后将对应 cronJob 入队。

**deleteJob**：解析 ownerRef，找到 cronJob 后入队。

### cronJob 事件回调（V2）

```go
cronJobsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc:    func(obj interface{}) { jm.enqueueController(obj) },
    UpdateFunc: jm.updateCronJob,
    DeleteFunc: func(obj interface{}) { jm.enqueueController(obj) },
})
```

**updateCronJob**（cronjob_controllerv2.go:358）：
1. 获取新老 cronJob 对象
2. 判断新老 `Spec.Schedule` 时间是否变化
   - 若发生变化：解析新的调度时间，计算下一次执行时间作为延迟入队时间，调 `enqueueControllerAfter(curr, *t)`
   - 若 Schedule 之外的参数变化：直接立即入队

### V2 的 Run 和 worker

```go
// pkg/controller/cronjob/cronjob_controllerv2.go:121
func (jm *ControllerV2) Run(workers int, stopCh <-chan struct{}) {
    defer utilruntime.HandleCrash()
    klog.InfoS("Starting cronjob controller v2")
    if !cache.WaitForNamedCacheSync(..., jm.jobListerSynced, jm.cronJobListerSynced) {
        return
    }
    for i := 0; i < workers; i++ {
        go wait.Until(jm.worker, time.Second, stopCh)
    }
    <-stopCh
}
```

worker 沿用 job 中的 `for next` 模式：

```go
func (jm *ControllerV2) worker() {
    for jm.processNextWorkItem() { }
}

func (jm *ControllerV2) processNextWorkItem() bool {
    key, quit := jm.queue.Get()
    if quit { return false }
    defer jm.queue.Done(key)
    requeueAfter, err := jm.sync(key.(string))
    // ...
}
```

### sync 函数

```go
// pkg/controller/cronjob/cronjob_controllerv2.go:163
func (jm *ControllerV2) sync(cronJobKey string) (*time.Duration, error) {
    ns, name, _ := cache.SplitMetaNamespaceKey(cronJobKey)
    cronJob, err := jm.cronJobLister.CronJobs(ns).Get(name)
    // ...
    jobsToBeReconciled, _ := jm.getJobsToBeReconciled(cronJob)
    cronJobCopy, requeueAfter, err := jm.syncCronJob(cronJob, jobsToBeReconciled)
    // ...
    err = jm.cleanupFinishedJobs(cronJobCopy, jobsToBeReconciled)
    // ...
}
```

**getJobsToBeReconciled**：通过 informer 中配置的 cronjob 的 label 选择器过滤出有 ownerRef 的 job，这些 job 都需要被协调同步。

---

## §06. CronJobController 同步核心 syncCronJob 源码解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 核心同步函数 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `syncCronJob:399` |
| 清理已完成 job | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `cleanupFinishedJobs:619` |
| 计算下次调度时间 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `nextScheduledTimeDuration:613` |
| active 列表操作 | [cronjob_controllerv2.go](kubernetes/pkg/controller/cronjob/cronjob_controllerv2.go) | `isJobInActiveList:682` |

### syncCronJob 主流程

**准备阶段**：
```go
cj = cj.DeepCopy()
now := jm.now()
childrenJobs := make(map[types.UID]bool)
```

**开始遍历 job（传入的 js 列表）**，对每个 job 有三个判断分支：

**判断分支 1 — 不属于这个 cronJob 的 job**：
- 若 `!found && !IsJobFinished(j)` — 产生 `UnexpectedJob` event（warning）

**判断分支 2 — 从 cronJob active 数组中删除已完成的 job**：
- 若 `found && IsJobFinished(j)` — 产生 `SawCompletedJob` event，调 `deleteFromActiveList`（重建不包含该 UID 的 newActive 数组，赋给 `cj.Status.Active`）

**判断分支 3 — 更新 cronJob 的 LastSuccessfulTime**：
- 若 job 不在 active 但处于完成状态，用 `j.Status.CompletionTime` 更新 `cj.Status.LastSuccessfulTime`

**清理 active 数组中已不存在的 job**：
- 遍历 active 数组中的 job，若在传入的 js 构造的 map 中存在则跳过
- 直接从 apiserver 中 GET 最新 job 信息，避免因 watch 响应慢导致本地 informer 信息未更新
- 若 apiserver 中也没有这个 job，更新 active 数组（删除该 UID）

**更新 cronJob 状态到 apiserver**：
```go
updatedCJ, err := jm.cronJobControl.UpdateStatus(cj)
```
同时判断 `DeletionTimestamp` 不为空则不进行后续创建 job 操作。

**处理 Suspend**：
```go
if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
    return cj, nil, nil  // 不创建新 job
}
```

### 解析下次的调度时间

```go
sched, err := cron.ParseStandard(*cj.Spec.Schedule)
scheduledTime, err := getNextScheduleTime(*cj, now, sched, jm.recorder)
```

**getNextScheduleTime 解析**：

1. **计算 earliestTime**：若 `cj.Status.LastScheduleTime != nil` 使用 `LastScheduleTime`，否则使用 `cj.ObjectMeta.CreationTimestamp.Time`

2. **StartingDeadlineSeconds 限制回溯窗口**：若配置了 `StartingDeadlineSeconds`，`earliestTime = max(earliestTime, now - StartingDeadlineSeconds)`，防止系统重启后追补过多历史调度

3. **检测错过次数**：若 `numberOfMissedSchedules > 100`，产生 `TooManyMissedTimes` event

4. **边界判断**：`scheduledTime == nil` — 说明队列已被填满，这是重启后 queue 马上被填满的边界条件，返回 `nextScheduledTimeDuration(sched, now)` 等待下次

### 判断此次调度任务是否已经完成

```go
if isJobInActiveList(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{
    Name:      getJobName(cj, *scheduledTime),
    Namespace: cj.Namespace,
}}, cj.Status.Active) || cj.Status.LastScheduleTime.Equal(&metav1.Time{Time: *scheduledTime}) {
    // 已调度，等待下次
    t := nextScheduledTimeDuration(sched, now)
    return cj, t, nil
}
```

判断依据：根据下次调度时间拼接的 job 名称在 active 列表中找到，就说明这次已经调度了；或者 `LastScheduleTime` 等于 `scheduledTime`。

### CronJob 并发策略

**Forbid（禁止并发）**：
```go
if spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(cj.Status.Active) > 0 {
    jm.recorder.Eventf(cj, corev1.EventTypeNormal, "JobAlreadyActive", "...")
    t := nextScheduledTimeDuration(sched, now)
    return cj, t, nil
}
```

**Replace（替换）**：
```go
if cj.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
    for _, j := range cj.Status.Active {
        job, _ := jm.jobControl.GetJob(j.Namespace, j.Name)
        if !deleteJob(cj, job, jm.jobControl, jm.recorder) {
            return cj, nil, fmt.Errorf("could not replace job %s/%s", ...)
        }
    }
}
```

### 创建 job

```go
jobReq, err := getJobFromTemplate2(cj, *scheduledTime)
jobResp, err := jm.jobControl.CreateJob(cj.Namespace, jobReq)
// 记录调度的耗时、打印日志、产生 SuccessfulCreate event
// 更新 cj.Status.Active + LastScheduleTime
jm.cronJobControl.UpdateStatus(cj)
```

job 名称由 `getJobName(cj, scheduledTime)` 生成（cronJob 名 + scheduledTime 的 hash），保证同一调度时间只会创建一个 job。

### cleanupFinishedJobs

```go
// pkg/controller/cronjob/cronjob_controllerv2.go:619
func (jm *ControllerV2) cleanupFinishedJobs(cj *batchv1.CronJob, js []*batchv1.Job) error {
    if cj.Spec.FailedJobsHistoryLimit == nil && cj.Spec.SuccessfulJobsHistoryLimit == nil {
        return nil  // 两个 limit 都没设置就不做清理
    }
    failedJobs, successfulJobs := []batchv1.Job{}, []batchv1.Job{}
    for _, j := range js {
        isFinished, finishedStatus := jm.getFinishedStatus(j)
        if isFinished && finishedStatus == batchv1.JobComplete {
            successfulJobs = append(successfulJobs, *j)
        } else if isFinished && finishedStatus == batchv1.JobFailed {
            failedJobs = append(failedJobs, *j)
        }
    }
    if cj.Spec.SuccessfulJobsHistoryLimit != nil {
        jm.removeOldestJobs(cj, successfulJobs, *cj.Spec.SuccessfulJobsHistoryLimit)
    }
    if cj.Spec.FailedJobsHistoryLimit != nil {
        jm.removeOldestJobs(cj, failedJobs, *cj.Spec.FailedJobsHistoryLimit)
    }
    // 最后更新一下状态，同步到 apiserver
    _, err = jm.cronJobControl.UpdateStatus(cj)
    return err
}
```

**一句话总结 syncCronJob**：syncCronJob 就是把新增删除的逻辑写在了一起，统一入口是好处（代码集中），逻辑较乱是代价（需要仔细区分新增/删除路径）。
