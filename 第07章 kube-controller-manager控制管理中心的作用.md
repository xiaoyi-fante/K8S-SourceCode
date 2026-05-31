# 第07章 kube-controller-manager 控制管理中心的作用

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 7 章 — kube-controller-manager 控制管理中心
> **源码入口**: `cmd/kube-controller-manager/app/controllermanager.go`

---

## 核心机制一览

1. **cobra 启动链 → Config → Run**：入口与 scheduler/apiserver 一脉相承。cobra `Run` 回调调用 `s.Config` 构建配置，再调 `Run(c.Complete(), ...)` 进入主循环。`Config` 内部依次完成校验、构建 kubeconfig、创建 clientset、创建 eventRecorder，最后 `ApplyTo` 将选项写入各 controller 的 componentConfig。

2. **NewControllerInitializers 注册控制器 map**：所有控制器以 `name → InitFunc` 的形式注册进一个 map。`KnownControllers` 返回 name 列表；`ControllerDisabledByDefault` 列举默认禁止的控制器（如 `bootstrapsigner`/`tokencleaner`）。`saTokenController` 因为其他控制器需要它构造 token，单独先于 map 注册。

3. **StartControllers：先启 SA Token Controller，再遍历 map 启动其余控制器**：每个控制器调用自己的 `initFn(ctx)`，通过 `ctx.IsControllerEnabled` 过滤禁用项，通过 `time.Sleep(jitter)` 错开启动时间防止雪崩。

4. **控制器模式：对比期望数量与当前运行数量**：所有控制器的核心逻辑一致——通过 Informer 获取当前运行数量，与 spec 中期望数量对比，不足则扩容，超额则缩容。

5. **ReplicaSetController 扩容采用渐进式 slowStartBatch**：从 1 开始按 2 倍增加每批创建数量，防止一次性创建大量相同配置的 Pod 同时失败，给 apiserver 造成压力。

6. **ReplicaSetController 缩容按 8 维优先级排序选 Pod 删除**：排序依据依次为：未分配 Node → Pod Phase（Pending < Unknown < Running）→ Not Ready < Ready → 删除代价（pod-deletion-cost annotation）→ 同节点上多开的 → Ready 时间短的 → 重启次数多的 → 创建时间短的。尽量删最近创建、状态不稳定的 Pod。

---

## 全章调用链总图

```
cmd/kube-controller-manager/main.go
  │
  ▼ cobra Run 回调                         controllermanager.go
  │
  ├── s.Config(KnownControllers(),          options.go:419
  │           ControllersDisabledByDefault.List())
  │     ├── s.Validate()                   → 校验 allControllers / disabledByDefault
  │     ├── BuildConfigFromFlags()          → 构建 kubeconfig
  │     ├── clientset.NewForConfig()        → 创建 kube client
  │     ├── createRecorder()               → 创建 eventRecorder
  │     ├── &kubecontrollerconfig.Config{} → 组装 Config 结构体
  │     └── s.ApplyTo(c)                   → 各 controller 的 componentConfig 写入
  │
  └── Run(c.Complete(), wait.NeverStop)    controllermanager.go:173
        ├── configz.New(ConfigzName)        → 注册 /configz 端点
        ├── 健康检查 + HTTP 服务器启动
        ├── leaderElectAndRun()             → 竞选 leader（复用第 06 章机制）
        │     └── OnStartedLeading → run()
        │           ├── CreateControllerContext()
        │           ├── initializersFunc()  → NewControllerInitializers(loopMode)
        │           └── StartControllers()  controllermanager.go:536
        │                 ├── startSATokenController()  → 最先启动
        │                 └── for name, initFn := range controllers
        │                       └── ctrl, started, err := initFn(ctx)
        │
        └── InformerFactory.Start() + ObjectOrMetadataInformerFactory.Start()
```

---

## §01. controller-manager 启动主流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| cobra Run 回调入口 | [controllermanager.go](kubernetes/cmd/kube-controller-manager/app/controllermanager.go) | `Run:173` |
| 已知控制器列表 | [controllermanager.go](kubernetes/cmd/kube-controller-manager/app/controllermanager.go) | `KnownControllers:379` |
| 控制器 name→InitFunc map | [controllermanager.go](kubernetes/cmd/kube-controller-manager/app/controllermanager.go) | `NewControllerInitializers:405` |
| 构建 Config 配置 | [options.go](kubernetes/cmd/kube-controller-manager/app/options/options.go) | `Config:419` |
| 启动所有控制器 | [controllermanager.go](kubernetes/cmd/kube-controller-manager/app/controllermanager.go) | `StartControllers:536` |

本节覆盖全章调用链总图从 `main()` 到 `StartControllers` 的完整路径，各步骤详见下文。

### Config：构建 controller-manager 的配置

入口调用：

```go
// cmd/kube-controller-manager/app/controllermanager.go（cobra Run 回调）
c, err := s.Config(KnownControllers(), ControllersDisabledByDefault.List())
if err != nil { os.Exit(1) }
if err := Run(c.Complete(), wait.NeverStop); err != nil { os.Exit(1) }
```

`Config` 的参数解析：
- **`KnownControllers()`**：返回所有已知控制器的名字（string 数组）
- **`ControllersDisabledByDefault`**：是一个 `sets.String`，只包含 `bootstrapsigner` 和 `tokencleaner` 两个默认禁止的控制器

`Config` 内部（`options.go:419`）依次执行：

```go
// cmd/kube-controller-manager/app/options/options.go:419
func (s KubeControllerManagerOptions) Config(allControllers []string,
    disabledByDefaultControllers []string) (*kubecontrollerconfig.Config, error) {

    // 1. 校验：allControllers 和 disabledByDefault 中的名字是否合法
    if err := s.Validate(allControllers, disabledByDefaultControllers); err != nil {
        return nil, err
    }

    // 2. 构建 kubeconfig
    kubeconfig, err := clientcmd.BuildConfigFromFlags(s.Master, s.Kubeconfig)
    kubeconfig.DisableCompression = true
    kubeconfig.QPS = s.Generic.ClientConnection.QPS
    kubeconfig.Burst = int(s.Generic.ClientConnection.Burst)

    // 3. 创建 clientset
    client, err := clientset.NewForConfig(
        restclient.AddUserAgent(kubeconfig, KubeControllerManagerUserAgent))

    // 4. 创建 eventRecorder
    eventRecorder := createRecorder(client, KubeControllerManagerUserAgent)

    // 5. 组装 Config，执行 ApplyTo
    c := &kubecontrollerconfig.Config{
        Client:        client,
        Kubeconfig:    kubeconfig,
        EventRecorder: eventRecorder,
    }
    if err := s.ApplyTo(c); err != nil { return nil, err }
    return c, nil
}
```

`ApplyTo` 内部依次调用各个 controller 选项的 `ApplyTo` 方法，例如：

```go
// options.go（ApplyTo 内部）
if err := s.StatefulSetController.ApplyTo(&c.ComponentConfig.StatefulSetController); err != nil {
    return err
}
```

`componentConfig` 里的值最终通过 `/configz` 端点可以实时查看（见下文）。

### KnownControllers 与 NewControllerInitializers

```go
// controllermanager.go:379
func KnownControllers() []string {
    ret := sets.StringKeySet(NewControllerInitializers(IncludeCloudLoops))
    // saTokenController 属于非常规初始化的控制器，单独 Insert 到 set 中
    ret.Insert(saTokenControllerName)
    return ret.List()
}
```

```go
// controllermanager.go:405
// NewControllerInitializers 是 name → InitFunc 的公共 map
func NewControllerInitializers(loopMode ControllerLoopMode) map[string]InitFunc {
    controllers := map[string]InitFunc{}
    controllers["endpoint"]              = startEndpointController
    controllers["endpointslice"]         = startEndpointSliceController
    controllers["replicationcontroller"] = startReplicationController
    controllers["replicaset"]            = startReplicaSetController
    controllers["deployment"]            = startDeploymentController
    controllers["statefulset"]           = startStatefulSetController
    controllers["namespace"]             = startNamespaceController
    controllers["job"]                   = startJobController
    // ... 共 30+ 个控制器
    return controllers
}
```

map 的 key 是控制器名，value 是启动函数。这种注册方式使得新增控制器只需往 map 里加一行，无需修改 `StartControllers` 的调度逻辑。

### Run：初始化 configz 与启动 leader election

```go
// controllermanager.go:173
func Run(c *config.CompletedConfig, stopCh <-chan struct{}) error {
    // 初始化 configz，注册 /configz 端点
    if cfgz, err := configz.New(ConfigzName); err == nil {
        cfgz.Set(c.ComponentConfig)
    }

    // 健康监测与 HTTP 服务器
    var checks []healthz.HealthChecker
    if c.ComponentConfig.Generic.LeaderElection.LeaderElect {
        electionChecker = leaderelection.NewLeaderHealthzAdaptor(time.Second * 20)
        checks = append(checks, electionChecker)
    }
    unsecuredMux = genericcontrollermanager.NewBaseHandler(...)
    if c.SecureServing != nil {
        c.SecureServing.Serve(handler, 0, stopCh)
    }

    // 正常开启 leader election，通过抢锁成功触发 OnStartedLeading 回调
    go leaderElectAndRun(c, id, electionChecker,
        c.ComponentConfig.Generic.LeaderElection.ResourceLock,
        c.ComponentConfig.Generic.LeaderElection.ResourceName,
        leaderelection.LeaderCallbacks{
            OnStartedLeading: run,        // 成为 leader 后执行
            OnStoppedLeading: func() {
                klog.Fatalf("leaderelection lost")
            },
        })
    select {}  // 主 goroutine 永久阻塞，所有工作由子 goroutine 完成
}
```

`/configz` 端点可用 `curl` 实时查看配置：

```bash
TOKEN=$(kubectl -n kube-system get secret $(kubectl -n kube-system get serviceaccount prometheus -o jsonpath='{.secrets[0].name}') -o jsonpath='{.data.token}' | base64 -d)
curl -k -s https://localhost:10257/configz --header "Authorization: Bearer $TOKEN" | python -m json.tool
```

输出示例（部分）：

```json
{
  "kubecontrollermanager.config.k8s.io": {
    "AttachDetachController": {
      "DisableAttachDetachReconcilerSync": false,
      "ReconcilerSyncLoopPeriod": "1m0s"
    },
    "CSRSigningController": {
      "ClusterSigningCertFile": "/etc/kubernetes/pki/ca.crt",
      "ClusterSigningDuration": "876h0m0s"
    }
  }
}
```

### run 函数：核心的启动方法

```go
// controllermanager.go（run 函数，OnStartedLeading 回调）
run := func(ctx context.Context, startSATokenController InitFunc, initializersFunc ControllerInitializersFunc) {
    // 构建 ControllerContext（含 InformerFactory、clientBuilder 等）
    controllerContext, err := CreateControllerContext(c, rootClientBuilder, clientBuilder, ctx.Done())
    if err != nil {
        klog.Fatalf("error building controller context: %v", err)
    }

    // 获取所有控制器的 initFn map
    controllerInitializers := initializersFunc(controllerContext.LoopMode)

    // 启动所有控制器
    if err := StartControllers(controllerContext, startSATokenController,
        controllerInitializers, unsecuredMux); err != nil {
        klog.Fatalf("error starting controllers: %v", err)
    }

    // 启动所有 informer
    controllerContext.InformerFactory.Start(controllerContext.Stop)
    controllerContext.ObjectOrMetadataInformerFactory.Start(controllerContext.Stop)
    close(controllerContext.InformersStarted)

    select {}  // 永久阻塞
}
```

### StartControllers：遍历控制器 map 并启动

```go
// controllermanager.go:536
func StartControllers(ctx ControllerContext, startSATokenController InitFunc,
    controllers map[string]InitFunc, unsecuredMux *mux.PathRecorderMux) error {

    // 1. 最先启动 SA Token Controller
    // 因为其他控制器需要它来构造 token
    if startSATokenController != nil {
        if _, _, err := startSATokenController(ctx); err != nil {
            return err
        }
    }

    // 2. 遍历所有注册的控制器，依次启动
    for controllerName, initFn := range controllers {
        if !ctx.IsControllerEnabled(controllerName) {
            klog.Warningf("%q is disabled", controllerName)
            continue
        }

        // 错开启动时间，防止雪崩
        time.Sleep(wait.Jitter(ctx.ComponentConfig.Generic.ControllerStartInterval.Duration, ControllerStartJitter))

        klog.V(1).Infof("Starting %q", controllerName)
        ctrl, started, err := initFn(ctx)
        if err != nil {
            klog.Errorf("Error starting %q", controllerName)
            return err
        }
        if !started {
            klog.Warningf("Skipping %q", controllerName)
            continue
        }
        if ctrl != nil {
            // 如果 controller 支持 debug handler，注册到 /debug/controllers/ 路径
            if debuggable, ok := ctrl.(controller.Debuggable); ok {
                if debugHandler := debuggable.DebuggingHandler(); debugHandler != nil {
                    basePath := "/debug/controllers/" + controllerName
                    unsecuredMux.UnlistHandle(basePath, ...)
                }
            }
        }
        klog.Infof("Started %q", controllerName)
    }
    return nil
}
```

`saTokenController` 单独优先启动的原因：其他控制器（如 deployment、replicaset）在操作资源时，需要 serviceaccount token 才能与 apiserver 交互，而 saTokenController 负责为 ServiceAccount 生成 token。如果先启动其他控制器，它们可能无法获取 token 而失败。

---

## §02. ReplicaSet 和对应的 ReplicaSetController 控制器

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 注册 replicaset 控制器 | [apps.go](kubernetes/cmd/kube-controller-manager/app/apps.go) | `startReplicaSetController:69` |
| 控制器构造 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `NewReplicaSetController:112` |
| 底层构造（注册 EventHandler） | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `NewBaseController:129` |
| 入队函数 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `enqueueRS:264` |
| worker 主循环 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `worker:514` |
| 处理函数 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `processNextWorkItem:519` |
| 核心调和逻辑 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `syncReplicaSet:646` |
| 管理副本数量 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `manageReplicas:541` |
| 渐进式扩容 | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `slowStartBatch:741` |
| 缩容选 Pod | [replica_set.go](kubernetes/pkg/controller/replicaset/replica_set.go) | `getPodsToDelete:800` |
| 缩容排序策略 | [controller_utils.go](kubernetes/pkg/controller/controller_utils.go) | `ActivePodsWithRanks.Less:844` |

### 本节局部调用链

```
StartControllers → initFn("replicaset") = startReplicaSetController
  │
  ▼ startReplicaSetController(ctx)          apps.go:69
  │   └── replicaset.NewReplicaSetController(
  │             ctx.InformerFactory.Apps().V1().ReplicaSets(),
  │             ctx.InformerFactory.Core().V1().Pods(),
  │             ctx.ClientBuilder.ClientOrDie("replicaset-controller"),
  │             replicaset.BurstReplicas,
  │         ).Run(workers, ctx.Stop)
  │
  ▼ NewBaseController()                     replica_set.go:129
  │   ├── rsInformer.AddEventHandler(AddFunc: rsc.addRS, ...)
  │   ├── podInformer.AddEventHandler(AddFunc: rsc.addPod, ...)
  │   └── rsc.syncHandler = rsc.syncReplicaSet
  │
  ▼ Run(workers, stopCh)
  │   ├── cache.WaitForNamedCacheSync()     → 等待 informer 缓存就绪
  │   └── for i := 0; i < workers; i++
  │         go wait.Until(rsc.worker, ...)
  │
  ▼ worker() → processNextWorkItem()
  │   └── rsc.syncHandler(key)              → syncReplicaSet(key)
  │
  ▼ syncReplicaSet(key)                     replica_set.go:646
  │   ├── rsc.rsLister.ReplicaSets(ns).Get(name)   → 从 informer 读 rs
  │   ├── metav1.LabelSelectorAsSelector()          → 获取 selector
  │   ├── rsc.podLister.Pods(ns).List(Everything()) → 列出 ns 内所有 pod
  │   ├── controller.FilterActivePods()             → 过滤非活跃 pod
  │   ├── rsc.claimPods(rs, selector, filteredPods) → 按 selector 认领 pod
  │   └── rsc.manageReplicas(filteredPods, rs)
  │         ├── diff < 0 → 扩容：slowStartBatch() → podControl.CreatePods()
  │         └── diff > 0 → 缩容：getPodsToDelete() → podControl.DeletePod()
```

### startReplicaSetController：准备工作

```go
// cmd/kube-controller-manager/app/apps.go:69
func startReplicaSetController(ctx ControllerContext) (http.Handler, bool, error) {
    go replicaset.NewReplicaSetController(
        ctx.InformerFactory.Apps().V1().ReplicaSets(),
        ctx.InformerFactory.Core().V1().Pods(),
        ctx.ClientBuilder.ClientOrDie("replicaset-controller"),
        replicaset.BurstReplicas,
    ).Run(int(ctx.ComponentConfig.ReplicaSetController.ConcurrentRSSyncs), ctx.Stop)
    return nil, true, nil
}
```

### NewBaseController：注册 EventHandler

```go
// pkg/controller/replicaset/replica_set.go:129
func NewBaseController(rsInformer, podInformer, kubeClient, burstReplicas, ...) *ReplicaSetController {
    eventBroadcaster := record.NewBroadcaster()
    eventBroadcaster.StartRecordingToSink(...)

    rsc := &ReplicaSetController{
        GroupVersionKind: gvk,
        kubeClient:       kubeClient,
        podControl:       controller.RealPodControl{
            KubeClient: kubeClient,
            Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{...}),
        },
    }

    // 监听 RS 对象变化：新增、更新、删除 → 入队
    rsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    rsc.addRS,
        UpdateFunc: rsc.updateRS,
        DeleteFunc: rsc.deleteRS,
    })

    // 监听 Pod 对象变化：每次 pod 变化都会触发关联 RS 的同步
    podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc:    rsc.addPod,
        // Pod 的每次变化都会触发关联 RS 的检查（比如 host assignment 变化）
        UpdateFunc: rsc.updatePod,
        DeleteFunc: rsc.deletePod,
    })

    rsc.syncHandler = rsc.syncReplicaSet  // 设置 syncHandler
    return rsc
}
```

`addRS` 的底层就是往队列中添加一条 key：

```go
// replica_set.go:264
func (rsc *ReplicaSetController) enqueueRS(rs *apps.ReplicaSet) {
    key, err := controller.KeyFunc(rs)
    if err != nil { return }
    rsc.queue.Add(key)
}
```

### Run 与 worker：消费队列

```go
// replica_set.go（Run 函数）
func (rsc *ReplicaSetController) Run(workers int, stopCh <-chan struct{}) {
    // 等待 informer 缓存同步完成
    if !cache.WaitForNamedCacheSync(rsc.Kind, stopCh,
        rsc.podListerSynced, rsc.rsListerSynced) {
        return
    }
    // 启动并发限制数量的 worker
    for i := 0; i < workers; i++ {
        go wait.Until(rsc.worker, time.Second, stopCh)
    }
    <-stopCh
}
```

`rsc.podListerSynced = podInformer.Informer().HasSynced`，`HasSynced` 返回 true 代表 informer 已完成首次 List，缓存有数据了，才能开始主流程。

worker 死循环：

```go
// replica_set.go:514
func (rsc *ReplicaSetController) worker() {
    for rsc.processNextWorkItem() {}
}

// replica_set.go:519
func (rsc *ReplicaSetController) processNextWorkItem() bool {
    key, quit := rsc.queue.Get()
    if quit { return false }
    defer rsc.queue.Done(key)

    err := rsc.syncHandler(key.(string))
    if err == nil {
        rsc.queue.Forget(key)  // 成功：从队列移除，不再 retry
        return true
    }

    utilruntime.HandleError(fmt.Errorf("sync %q failed with %v", key, err))
    rsc.queue.AddRateLimited(key)  // 失败：其他 worker 还要拿到这个 key 进行重试
    return true
}
```

设计要点：`syncHandler` 是幂等的。失败时 `AddRateLimited` 而非立即重试，避免错误风暴。

### syncReplicaSet：核心调和逻辑

```go
// replica_set.go:646
func (rsc *ReplicaSetController) syncReplicaSet(key string) error {
    namespace, name, err := cache.SplitMetaNamespaceKey(key)

    // 1. 从 rs informer 获取 rs 对象
    rs, err := rsc.rsLister.ReplicaSets(namespace).Get(name)
    if apierrors.IsNotFound(err) {
        rsc.expectations.DeleteExpectations(key)
        return nil
    }

    // 2. 获取 rs 的 selector（标签选择器）
    selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)

    // 3. 列出 ns 内所有 pod，过滤 inactive pod，再按 selector 认领
    allPods, err := rsc.podLister.Pods(rs.Namespace).List(labels.Everything())
    filteredPods := controller.FilterActivePods(allPods)
    filteredPods, err = rsc.claimPods(rs, selector, filteredPods)

    // 4. 调用 manageReplicas 做实际的增删
    // rsNeedsSync：基于 expectations 机制判断是否真正需要执行同步
    // expectations 记录了"已发出但尚未被 informer 观察到"的创建/删除期望数量
    // 如果上一轮发出的 CreatePod 还没有被 informer 反馈回来，就跳过本轮同步
    // 防止在 informer 延迟回调期间重复扩/缩容
    var manageReplicasErr error
    if rsNeedsSync && rs.DeletionTimestamp == nil {
        manageReplicasErr = rsc.manageReplicas(filteredPods, rs)
    }

    // 5. 更新 rs status
    newStatus := calculateStatus(rs, filteredPods, manageReplicasErr)
    // ...
}
```

### manageReplicas：扩容与缩容的分支

```go
// replica_set.go:541
func (rsc *ReplicaSetController) manageReplicas(filteredPods []*v1.Pod, rs *apps.ReplicaSet) error {
    // 用当前运行 pod 数减去 rs 配置的副本数得到 diff
    diff := len(filteredPods) - int(*(rs.Spec.Replicas))

    if diff < 0 {
        // 扩容：diff 取绝对值即为需要新建的数量
        diff *= -1
        // slowStartBatch 渐进式创建，防止大量错误同时涌现
        successfulCreations, err := slowStartBatch(diff, controller.SlowStartInitialBatchSize,
            func() error {
                return rsc.podControl.CreatePods(rs.Namespace, &rs.Spec.Template, rs, metaOwner)
            })
        // ...
    } else if diff > 0 {
        // 缩容：选出要删除的 pod，并发删除
        relatedPods, _ := rsc.getIndirectlyRelatedPods(rs)
        podsToDelete := getPodsToDelete(filteredPods, relatedPods, diff)
        var wg sync.WaitGroup
        wg.Add(diff)
        for _, pod := range podsToDelete {
            go func(targetPod *v1.Pod) {
                defer wg.Done()
                rsc.podControl.DeletePod(rs.Namespace, targetPod.Name, rs)
            }(pod)
        }
        wg.Wait()
    }
}
```

### 渐进式扩容：slowStartBatch

```go
// replica_set.go:741
func slowStartBatch(count int, initialBatchSize int, fn func() error) (int, error) {
    remaining := count
    successes := 0
    for batchSize := integer.IntMin(remaining, initialBatchSize); batchSize > 0; batchSize = integer.IntMin(2*batchSize, remaining) {
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
        if len(errCh) > 0 {
            return successes, <-errCh
        }
        remaining -= batchSize
    }
    return successes, nil
}
```

以扩容 10 个 Pod 为例（`initialBatchSize = 1`）：

| 轮次 | batchSize 计算 | 本轮创建数 | 累计完成 |
|------|--------------|-----------|---------|
| 第 1 轮 | min(10, 1) = 1 | 1 | 1 |
| 第 2 轮 | min(9, 2) = 2 | 2 | 3 |
| 第 3 轮 | min(7, 4) = 4 | 4 | 7 |
| 第 4 轮 | min(3, 8) = 3 | 3 | 10，结束 |

每轮 batchSize 翻倍，目的是防止全量扩容时大量相同配置的 Pod 同时出现相同错误，对 apiserver 和服务组件产生雪崩压力。若某轮出现错误，函数立即返回已成功数量 + 错误，RS 被重新入队等待下次同步，不会继续扩容。

真正的 create 操作通过 `rsc.podControl.CreatePods` → 底层调用 client 的 create pod，通过 apiserver 写入 etcd，等待 scheduler 调度。

### 缩容策略：getPodsToDelete 与排序

缩容时，并非随机删除 Pod，而是按 8 个优先级维度排序后，取前 `diff` 个删除。目的是尽量删最近创建、状态不稳定的 Pod。

```go
// replica_set.go:800
func getPodsToDelete(filteredPods, relatedPods []*v1.Pod, diff int) []*v1.Pod {
    if diff < len(filteredPods) {
        podsWithRanks := getPodsRankedByRelatedPodsOnSameNode(filteredPods, relatedPods)
        sort.Sort(podsWithRanks)
        reportSortingDeletionAgeRatioMetric(filteredPods, diff)
    }
    return filteredPods[:diff]
}
```

排序由 `ActivePodsWithRanks.Less`（`controller_utils.go:844`）实现，8 个策略依次比较，满足前一条即返回，否则继续下一条：

| 策略 | 判断逻辑 | 目的 |
|------|---------|------|
| 1 | 未分配 Node 的排前面 | Pending Pod 优先删，节省资源 |
| 2 | Phase: Pending < Unknown < Running | 状态越差排越前 |
| 3 | Not Ready < Ready | 不健康的优先删 |
| 4 | pod-deletion-cost annotation 小的排前面 | 用户可通过 annotation 保护重要 Pod |
| 5 | 同节点上多开的排前面 | 避免集中在同一节点 |
| 6 | Ready 时间短的排前面 | 刚变好的说明不稳定 |
| 7 | 重启次数多的排前面 | 频繁重启说明有问题 |
| 8 | 创建时间短的排前面 | 最近创建的代表最新一轮扩容 |

**pod-deletion-cost**（策略 4）是 v1.21 引入的功能，值是 int32 写在 Pod 的 Annotations 中。数字越小删除代价越小、越优先被删。同一应用的不同 Pod 可能利用率不同，在缩容时可以优先移除利用率较低的 Pod，避免频繁更新 Pod。

缩容动作最终通过 `rsc.podControl.DeletePod` 并发执行，`diff` 个 goroutine 同时删。

---
