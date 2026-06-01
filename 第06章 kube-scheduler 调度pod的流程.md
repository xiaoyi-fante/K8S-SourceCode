# 第06章 kube-scheduler 调度pod的流程

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 6 章 — kube-scheduler 调度 Pod 的流程
> **源码入口**: `cmd/kube-scheduler/scheduler.go`

---

## 核心机制一览

1. **cobra 入口 → runCommand → Setup + Run**：kube-scheduler 与 kubectl/apiserver 遵循相同的 cobra 启动模式。`main()` 构造命令树，`runCommand` 是真正的执行体，先调 `Setup` 拿到 `CompletedConfig` + `Scheduler`，再交给 `Run` 进入主循环。

2. **clientset 是一组 REST 客户端的集合**：`createClients` 返回的 `clientset.Interface` 内部持有针对每个 API Group/Version 的独立 HTTP 客户端（如 `CoreV1Client`、`AppsV1Client`），调用方通过 `clientset.CoreV1().Pods(...).List(...)` 统一访问，无需关心底层 REST 路径。

3. **leader election 基于 k8s API 的原子写实现分布式锁**：多副本部署时通过 Lease 资源竞选，只有持锁实例才执行调度主逻辑，丢锁时立即退出（`os.Exit(0)`）。三个参数控制锁的生命周期：`LeaseDuration`（租约时长）、`RenewDeadline`（更新截止）、`RetryPeriod`（非 leader 重试间隔）。

4. **Event 三角色：EventRecorder → EventBroadcaster → Sink**：EventRecorder 生成事件，EventBroadcaster 通过内部 channel 分发，`StartRecordingToSink` 注册的 watcher 负责聚合（相同 key 的事件合并计数）并通过 Create/Patch 写入 apiserver，存入 etcd。

5. **Informer 四件套：Reflector + DeltaFIFO + Informer + Indexer**：Reflector 对 apiserver 做 List+Watch，变更事件写入 DeltaFIFO 队列；`HandleDeltas` 串行执行两步——先更新 Indexer（路径 A），再 `processor.distribute` 分发 EventHandler 回调（路径 B），保证回调触发时 Indexer 已是最新；`sharedInformerFactory` 按资源类型缓存 Informer，同类型资源只创建一个 Watch 连接。

6. **SchedulingQueue 三子队列 + Assume/Bind 分离**：未调度 Pod 由 Informer EventHandler 异步推入 `SchedulingQueue`（`activeQ` 堆按优先级排序）；`scheduleOne` 从队列弹出后，Assume 同步写本地 cache（设置 `NodeName`，让后续调度感知该 Node 已占用），Bind 异步发请求到 apiserver，两步分离使主调度循环不阻塞。

7. **插件化调度框架：Filter + Score + 插件 registry**：调度算法分 Predict（Filter）和 Priority（Score）两阶段，每阶段调用注册的插件链；所有内置插件通过 `NewInTreeRegistry` 注册，每个插件实现 `framework.FilterPlugin` 或 `framework.ScorePlugin` 接口，scheduler 构造时按配置组装插件链，扩展调度策略无需修改核心代码。

---

## 全章调用链总图

从 `main()` 到 Pod 最终绑定到 Node 的完整路径：

```
scheduler.go: main()
  │
  ▼ NewSchedulerCommand()              server.go:64
  ▼ runCommand()                       server.go:120
  │
  ├── Setup()                          server.go:299
  │     ├── opts.Config()              options.go:254
  │     │     ├── createKubeConfig()
  │     │     ├── createClients()      → client + eventClient（两个 clientset）
  │     │     ├── makeLeaderElectionConfig() → Lease 锁配置
  │     │     └── NewInformerFactory() → sharedInformerFactory（按类型缓存 informer）
  │     └── scheduler.New()
  │           ├── newPodInformer()     → FieldSelector 只 Watch 非终态 Pod
  │           └── addAllEventHandlers()
  │                 ├── 已调度 Pod → addPodToCache（更新 schedulerCache）
  │                 └── 未调度 Pod → addPodToSchedulingQueue（入 PriorityQueue）
  │
  └── Run()                            server.go:136
        ├── EventBroadcaster.StartRecordingToSink()
        ├── InformerFactory.Start()    → 每个 informer 启动 goroutine
        │     └── sharedIndexInformer.Run()
        │           └── controller.Run()
        │                 ├── Reflector.Run()    → List+Watch → DeltaFIFO（生产者）
        │                 └── processLoop()      → HandleDeltas（消费者）
        │                       ├── indexer.Update/Add/Delete  → 路径 A：更新本地缓存
        │                       └── processor.distribute()     → 路径 B：触发 EventHandler
        ├── InformerFactory.WaitForCacheSync()   → 阻塞直到缓存就绪
        └── leaderelection.RunOrDie()
              └── OnStartedLeading → sched.Run(ctx)
                    └── wait.UntilWithContext(scheduleOne)
                          │
                          ▼ scheduleOne()        scheduler.go:441
                          │   ├── NextPod()      → SchedulingQueue.Pop()
                          │   ├── Algorithm.Schedule()
                          │   │     ├── snapshot()         → 快照 Node 状态
                          │   │     ├── findNodesThatFitPod() → RunFilterPlugins()
                          │   │     │     └── pl.Filter()  → 各插件（NodeName/Taint/...）
                          │   │     └── prioritizeNodes()  → RunScorePlugins() → selectHost()
                          │   ├── assume()       → AssumePod（写 cache，NodeName=host）
                          │   └── go bind()      → RunBindPlugins()
                          │         └── POST /binding → apiserver → etcd
                          │               └── kubelet Watch 到 → 拉镜像 → 启动容器
```

---

## §01. kube-scheduler 的启动流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| main 入口 | [scheduler.go](kubernetes/cmd/kube-scheduler/scheduler.go) | `main:33` |
| cobra 命令构造 | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `NewSchedulerCommand:64` |
| 命令执行体 | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `runCommand:120` |
| 配置 + scheduler 构建 | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `Setup:299` |
| 主循环（含 leader election） | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `Run:136` |
| 配置初始化（kubeconfig/clients/informer） | [options.go](kubernetes/cmd/kube-scheduler/app/options/options.go) | `Config:254` |
| kube clients 创建 | [options.go](kubernetes/cmd/kube-scheduler/app/options/options.go) | `createClients:357` |
| leader election 配置 | [options.go](kubernetes/cmd/kube-scheduler/app/options/options.go) | `makeLeaderElectionConfig:300` |
| InformerFactory 创建 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `NewInformerFactory:659` |
| Clientset 结构体 | [clientset.go](kubernetes/staging/src/k8s.io/client-go/kubernetes/clientset.go) | `Clientset:121` |

### 启动流程总图

```
scheduler.go: main()
  │
  ▼ app.NewSchedulerCommand()              → 构造 cobra.Command 树，注册所有 flag
  │   server.go:64
  │
  ▼ command.Execute()                      → cobra 解析 flag，路由到 runCommand
  │
  ▼ runCommand()                           server.go:120
  │   ├── Setup(ctx, opts)                 → 返回 CompletedConfig + Scheduler
  │   └── Run(ctx, cc, sched)              → 进入主循环，不再返回（除非出错）
  │
  ├── Setup()                              server.go:299
  │     ├── opts.Config()                  → options.go:254，构建原始 Config
  │     │     ├── createKubeConfig()       → 读 --kubeconfig，返回 *rest.Config
  │     │     ├── createClients()          → 返回 clientset + eventClient
  │     │     ├── makeLeaderElectionConfig()  → 构建分布式锁配置（可选）
  │     │     └── NewInformerFactory()     → 创建 SharedInformerFactory
  │     ├── c.Complete()                   → 填充默认值，返回 CompletedConfig
  │     └── scheduler.New(...)             → 构造 Scheduler 对象（含插件框架）
  │
  └── Run()                                server.go:136
        ├── configz.New("componentconfig") → 注册 /configz 端点
        ├── EventBroadcaster.StartRecordingToSink()
        ├── 启动 healthz/metrics 服务器
        └── leaderelection.RunOrDie()      → 竞选 leader，当选后执行 sched.Run()
```

### runCommand：Setup + Run 的组合器

```go
// cmd/kube-scheduler/app/server.go:120
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    cc, sched, err := Setup(ctx, opts, registryOptions...)  // 构建配置 + Scheduler
    if err != nil {
        return err
    }
    return Run(ctx, cc, sched)  // 进入主循环
}
```

`Setup` 负责所有初始化；`Run` 负责启动服务并进入调度主循环。两者分离便于测试中单独调用 `Setup` 校验配置。

### opts.Config()：三步初始化

```go
// cmd/kube-scheduler/app/options/options.go:254
func (o *Options) Config() (*schedulerappconfig.Config, error) {
    c := &schedulerappconfig.Config{}
    o.ApplyTo(c)

    // 1. 用 --kubeconfig 初始化 REST 配置
    kubeConfig, _ := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)

    // 2. 创建 kube clients（clientset）
    client, eventClient, _ := createClients(kubeConfig)

    // 3. 开启选举时，构建 leader election 锁配置
    if c.ComponentConfig.LeaderElection.LeaderElect {
        leaderElectionConfig, _ = makeLeaderElectionConfig(...)
    }

    c.Client = client
    c.InformerFactory = scheduler.NewInformerFactory(client, 0)  // 创建 informer 工厂
    c.LeaderElection = leaderElectionConfig
    return c, nil
}
```

### clientset 解读

```go
// cmd/kube-scheduler/app/options/options.go:357
func createClients(kubeConfig *restclient.Config) (clientset.Interface, clientset.Interface, error) {
    client, _ := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
    eventClient, _ := clientset.NewForConfig(kubeConfig)
    return client, eventClient, nil
}
```

返回两个 clientset：`client` 用于调度逻辑，`eventClient` 专门用于写 Event 记录。

`Clientset` 的本质是一个结构体，持有针对每个 API Group 的独立 REST 客户端：

```go
// staging/src/k8s.io/client-go/kubernetes/clientset.go:121
type Clientset struct {
    *discovery.DiscoveryClient
    appsV1           *appsv1.AppsV1Client
    authorizationV1  *authorizationv1.AuthorizationV1Client
    batchV1          *batchv1.BatchV1Client
    // ... 共 30+ 个字段，每个对应一个 API Group/Version
}
```

调用方通过方法访问：`clientset.CoreV1().Pods("ns").List(...)` → 底层是对 apiserver 的 HTTP GET 请求。

---

## §02. kube-scheduler 中的 leader election 选主机制解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| leader election 配置构建 | [options.go](kubernetes/cmd/kube-scheduler/app/options/options.go) | `makeLeaderElectionConfig:300` |
| 资源锁创建 | [interface.go](kubernetes/staging/src/k8s.io/client-go/tools/leaderelection/resourcelock/interface.go) | `New:100` |
| scheduler 中启动抢锁 | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `Run:136` |
| 抢锁主循环 | [leaderelection.go](kubernetes/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go) | `LeaderElector.Run:196` |
| 抢锁逻辑 | [leaderelection.go](kubernetes/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go) | `acquire:238` |
| 原子获取/续租 | [leaderelection.go](kubernetes/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go) | `tryAcquireOrRenew:312` |

### 为什么需要 leader election

在 Kubernetes 中，kube-scheduler、kube-controller-manager 等组件多副本部署时，如果多个实例同时执行调度逻辑，可能对同一个 Pod 做出冲突的调度决策。leader election 通过分布式锁保证任意时刻只有一个实例处于活跃状态，其余实例处于热备状态，leader 宕机后立即竞选新 leader，实现高可用。

### leader election 流程总图

```
Run() → cc.LeaderElection != nil
  │
  ▼ 注册 LeaderCallbacks
  │   ├── OnStartedLeading: close(waitingForLeader); sched.Run(ctx)  → 成为leader，执行调度
  │   ├── OnStoppedLeading: os.Exit(0)                               → 丢锁，立即退出进程
  │   └── OnNewLeader: 感知新leader变化（自己当选时不做事）
  │
  ▼ leaderelection.NewLeaderElector(*cc.LeaderElection)              → 构造选举器
  │
  ▼ leaderElector.Run(ctx)                                           leaderelection.go:196
  │
  ▼ le.acquire(ctx)                                                  leaderelection.go:238
  │   └── 每隔 RetryPeriod 调用 tryAcquireOrRenew()
  │         ├── Lock.Get()                    → 从 apiserver 读取当前 Lease 资源
  │         ├── 如果 IsNotFound → Lock.Create()  → 首次创建，自己成为 leader
  │         ├── 检查锁是否过期：HolderIdentity + LeaseDuration + Now
  │         │     └── 未过期且不是自己持有 → return false（继续等待）
  │         └── 自己持有 → Lock.Update()      → 乐观锁更新（resourceVersion 防并发冲突）
  │
  ▼ acquire 返回 true → 执行 OnStartedLeading 回调 → sched.Run(ctx)
```

### makeLeaderElectionConfig：锁的初始化

```go
// cmd/kube-scheduler/app/options/options.go:300
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration,
    kubeConfig *restclient.Config, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {

    hostname, _ := os.Hostname()
    // hostname + UUID 保证同一主机的多个进程 ID 不同
    id := hostname + "_" + string(uuid.NewUUID())

    rl, _ := resourcelock.NewFromKubeconfig(
        config.ResourceLock,        // 默认 "leases"
        config.ResourceNamespace,   // 默认 "kube-system"
        config.ResourceName,        // 默认 "kube-scheduler"
        resourcelock.ResourceLockConfig{Identity: id, EventRecorder: recorder},
        kubeConfig,
        config.RenewDeadline.Duration)

    return &leaderelection.LeaderElectionConfig{
        Lock:          rl,
        LeaseDuration: config.LeaseDuration.Duration,  // 默认 15s，租约时长
        RenewDeadline: config.RenewDeadline.Duration,  // 默认 10s，必须在此时间内续租
        RetryPeriod:   config.RetryPeriod.Duration,    // 默认 2s，非leader重试间隔
        WatchDog:      leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
        Name:          "kube-scheduler",
    }, nil
}
```

锁配置可通过 `/configz` 端点查看：

```json
"LeaderElection": {
    "LeaderElect": true,
    "LeaseDuration": "15s",
    "RenewDeadline": "10s",
    "ResourceLock": "leases",
    "ResourceName": "kube-scheduler",
    "ResourceNamespace": "kube-system",
    "RetryPeriod": "2s"
}
```

### resourcelock：三种资源锁实现

```go
// staging/src/k8s.io/client-go/tools/leaderelection/resourcelock/interface.go:100
func New(lockType string, ns string, name string, ...) (Interface, error) {
    endpointsLock := &EndpointsLock{...}
    configmapLock := &ConfigMapLock{...}
    leaseLock := &LeaseLock{...}

    switch lockType {
    case EndpointsResourceLock:
        return endpointsLock, nil
    case ConfigMapsResourceLock:
        return configmapLock, nil
    case LeasesResourceLock:
        return &LeaseLock{...}, nil
    // ...
    }
}
```

kube-scheduler 默认使用 `leases`（`coordination.k8s.io/v1` API），相比 Endpoints 和 ConfigMap，Lease 对象更轻量，对 etcd 压力更小，且对 Lease 的写操作不会触发不必要的事件通知。

### scheduler 中抢锁的运行

```go
// cmd/kube-scheduler/app/server.go:136（Run函数内）
if cc.LeaderElection != nil {
    cc.LeaderElection.Callbacks = leaderelection.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) {
            close(waitingForLeader)  // 通知 /healthz 已成为 leader
            sched.Run(ctx)           // 真正的调度主循环
        },
        OnStoppedLeading: func() {
            select {
            case <-ctx.Done():
                os.Exit(0)           // 优雅退出
            default:
                klog.Exitf("leaderelection lost")  // 意外丢锁，立即退出
            }
        },
    }
    leaderElector, _ := leaderelection.NewLeaderElector(*cc.LeaderElection)
    leaderElector.Run(ctx)
}
```

`OnStoppedLeading` 直接调 `os.Exit` 而不是 return，是因为 scheduler 的内部状态（informer 缓存、假设队列）可能已经被修改，继续运行比退出更危险。

### tryAcquireOrRenew：乐观锁原理

`tryAcquireOrRenew` 是抢锁的核心，利用 apiserver 的 **resourceVersion** 字段实现乐观并发控制：

```
tryAcquireOrRenew 执行步骤：
1. Lock.Get()   → 读取当前 Lease 资源（含 resourceVersion）
2. 如果 IsNotFound → Lock.Create() → 首次创建，成功即当选
3. 检查 HolderIdentity：
   ├── 不是自己 AND 锁未过期（observedTime + LeaseDuration > now）→ return false
   └── 自己持有 OR 锁已过期 → 准备更新
4. Lock.Update() → PUT 请求携带 resourceVersion
   ├── apiserver 校验 resourceVersion 是否还匹配
   ├── 匹配 → 写入成功，当选/续租成功
   └── 不匹配（并发冲突）→ 写入失败，return false
```

resourceVersion 来自 `ObjectMeta`，每次写操作后自动递增。携带 resourceVersion 的 Update 请求等价于"如果此时没有其他人修改过，则更新"，从而保证原子性，无需分布式锁中间件。

### 验证 leader election 效果

```bash
# 查看当前 leader（HOLDER 字段）
kubectl get lease -n kube-system kube-scheduler -o wide

# 模拟效果：启动 id=1 的实例抢锁
./leaderelection -kubeconfig=/root/.kube/config -id=1
# I0915 16:19:32.776828  314 leaderelection.go:258] successfully acquired lease default/example
# I0915 16:19:32.776935  314 leaderelection.go:81]  Controller loop...

# 再启动 id=2 的实例，会看到 id=1 是 leader
./leaderelection -kubeconfig=/root/.kube/config -id=2
# I0915 16:20:04.302847  39543 leaderelection.go:143] new leader elected: 1

# 停止 id=1，id=2 自动竞选成功
# I0915 16:22:01.047556  1801 leaderelection.go:258] successfully acquired lease default/example
```

---

## §03. k8s 的事件 event 和 kube-scheduler 中的事件广播器

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| EventBroadcaster 构造 | [event_broadcaster.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_broadcaster.go) | `NewEventBroadcasterAdapter:342` |
| recorderImpl 结构体 | [event_recorder.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_recorder.go) | `recorderImpl:35` |
| 开启广播（scheduler Run中） | [server.go](kubernetes/cmd/kube-scheduler/app/server.go) | `Run:136` |
| 事件分发到 sink | [event_broadcaster.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_broadcaster.go) | `StartRecordingToSink:327` |
| 消费 ResultChan | [event_broadcaster.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_broadcaster.go) | `StartEventWatcher:295` |
| 聚合 + 写 apiserver | [event_broadcaster.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_broadcaster.go) | `recordToSink:168` |
| 事件 key 生成 | [event_broadcaster.go](kubernetes/staging/src/k8s.io/client-go/tools/events/event_broadcaster.go) | `getKey:280` |
| scheduler 记录调度成功 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `finishBinding` |
| scheduler 记录调度失败 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `recordSchedulingFailure` |

### 什么是 k8s Event

Event 是 k8s 中用于展示集群内部发生事件的对象——如调度程序做了哪些决策、为什么某些 Pod 被驱逐。所有核心组件和扩展（operator）都可通过 API Server 创建事件：

```bash
kubectl get events -A   # 查看所有 namespace 的事件
kubectl describe pod <name>  # 在输出末尾可看到该 Pod 相关的 events
```

**存储特点**：Event 量大，只临时存在 etcd 中（通常保留 1 小时内的事件）；生产环境建议将 events 存储到独立 etcd 集群，与业务数据 etcd 分开，避免大量事件冲击存储。

### Event 三角色架构

```
k8s 组件（scheduler/kubelet/...）
  │
  ▼ EventRecorder.Eventf(...)         → 生成 Event，写入 EventBroadcaster 的 incoming channel
  │   recorder.generateEvent()
  │   recorder.Action()
  │                                         ┌─────────────────────────────┐
  │                                         │       EventBroadcaster       │
  │   ┌──────────────────────────────────► m.loop / c.incoming            │
  │   │                                    distribute()                   │
  │   │                                         │                          │
  │   │         ┌───────────────────────────────┘                          │
  │   │         │                                                           │
  │   │   ┌─────▼──────────┐    ┌──────────────────┐                      │
  │   │   │   c.result      │    │  StartRecordingToSink  → Filter/聚合/计数 │
  │   │   │   ResultChan    │◄──│  StartEventWatcher      → 消费 ResultChan │
  │   │   └────────────────┘    │  StartStructuredLogging → klog            │
  │   │                         └──────────────────┘                      │
  │   └─────────────────────────────────────────────────────────────────── │
  │                                                                         └──► apiserver Create/Patch
```

三个角色分工：
- **EventRecorder**：事件生成者，k8s 组件调用其方法生成事件
- **EventBroadcaster**：事件广播器，负责消费 EventRecorder 产生的事件，然后分发给各 watcher
- **broadcasterWatcher**：定义事件的处理方式，如上报 apiserver（`StartRecordingToSink`）或写 klog

### kube-scheduler 中的 Event 初始化

`Setup` 中创建 EventBroadcaster，`Run` 中开启广播：

```go
// options.go:254（Config函数中）
c.EventBroadcaster = events.NewEventBroadcasterAdapter(eventClient)

// server.go:284（getRecorderFactory）
recorderFactory := getRecorderFactory(&cc)

// server.go:136（Run函数中）
cc.EventBroadcaster.StartRecordingToSink(ctx.Done())  // 开启事件广播
```

`NewEventBroadcasterAdapter` 同时兼容新旧两套 Event API（`events.k8s.io/v1` 和 `core/v1`），内部创建一个 `eventBroadcasterImpl`，持有一个 `watch.Broadcaster`（带 buffer 的 channel 分发器）。

### StartRecordingToSink → startRecordingEvents

```go
// staging/src/k8s.io/client-go/tools/events/event_broadcaster.go:327
func (e *eventBroadcasterImpl) StartRecordingToSink(stopCh <-chan struct{}) {
    go wait.Until(e.refreshExistingEventSeries, refreshTime, stopCh)
    go wait.Until(e.finishSeries, finishTime, stopCh)
    e.startRecordingEvents(stopCh)
}

// event_broadcaster.go:310
func (e *eventBroadcasterImpl) startRecordingEvents(stopCh <-chan struct{}) {
    eventHandler := func(obj runtime.Object) {
        event, ok := obj.(*eventsv1.Event)
        e.recordToSink(event, clock.RealClock{})  // 聚合后写入 apiserver
    }
    stopWatcher := e.StartEventWatcher(eventHandler)
    go func() { <-stopCh; stopWatcher() }()
}
```

### StartEventWatcher：消费 ResultChan

```go
// event_broadcaster.go:295
func (e *eventBroadcasterImpl) StartEventWatcher(eventHandler func(event runtime.Object)) func() {
    watcher := e.Watch()
    go func() {
        defer utilruntime.HandleCrash()
        for {
            watchEvent, ok := <-watcher.ResultChan()
            if !ok { return }
            eventHandler(watchEvent.Object)  // 传给 recordToSink
        }
    }()
    return watcher.Stop
}
```

### recordToSink：聚合与写入

```go
// event_broadcaster.go:280（getKey）
func getKey(event *eventsv1.Event) eventKey {
    key := eventKey{
        action:              event.Action,
        reason:              event.Reason,
        reportingController: event.ReportingController,
        regarding:           event.Regarding,
    }
    if event.Related != nil { key.related = *event.Related }
    return key
}
```

`recordToSink` 用 `getKey` 生成事件分类键，在 `eventCache` 中查找同类事件：
- **找到**：更新 `Series.Count++` 和 `Series.LastObservedTime`，返回 nil（不创建新资源，改为 Patch 更新计数）
- **找不到**：创建新的 Event 对象，`Series.Count = 1`

最终调用 `EventSink.Create` 或 `EventSink.Patch` 写入 apiserver：

```go
// staging/src/k8s.io/client-go/tools/events/interfaces.go
type EventSink interface {
    Create(event *eventsv1.Event) (*eventsv1.Event, error)
    Update(event *eventsv1.Event) (*eventsv1.Event, error)
    Patch(oldEvent *eventsv1.Event, data []byte) (*eventsv1.Event, error)
}
```

### scheduler 在哪里记录 Event

**调度成功**（`finishBinding`）：

```go
// pkg/scheduler/scheduler.go
fwk.EventRecorder().Eventf(assumed, nil, v1.EventTypeNormal, "Scheduled", "Binding",
    "Successfully assigned %v/%v to %v", assumed.Namespace, assumed.Name, targetNode)
```

**调度失败**（`recordSchedulingFailure`）：

```go
// pkg/scheduler/scheduler.go
fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", msg)
```

`Eventf` 的参数分析：
- `regarding`：关注哪种资源的 event，这里是 pod
- `related`：还关联哪些资源，失败时为 nil
- `eventtype`：`v1.EventTypeWarning` 或 `v1.EventTypeNormal`
- `reason`：事件原因，如 `FailedScheduling`
- `action`：执行了哪个动作，如 `Scheduling`
- `note`：自由文本 msg

验证：给 Pod 添加不存在的 `nodeSelector: disktype: ssd`，Pod 无法调度，`kubectl get event` 会看到 `FailedScheduling` 事件；给节点打上标签后，调度成功，会看到 `Scheduled` 事件。

---

## §04. k8s 的 informer 机制

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| SharedInformerFactory 构造 | [factory.go](kubernetes/staging/src/k8s.io/client-go/informers/factory.go) | `NewSharedInformerFactory:96` |
| sharedInformerFactory 结构体 | [factory.go](kubernetes/staging/src/k8s.io/client-go/informers/factory.go) | `sharedInformerFactory:55` |
| Informer 启动 | [factory.go](kubernetes/staging/src/k8s.io/client-go/informers/factory.go) | `Start:128` |
| 注册 EventHandler | [shared_informer.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/shared_informer.go) | `AddEventHandler:456` |
| Informer Run | [shared_informer.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/shared_informer.go) | `sharedIndexInformer.Run:368` |
| Reflector 结构体 | [reflector.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/reflector.go) | `Reflector:49` |
| List+Watch 实现 | [reflector.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/reflector.go) | `ListAndWatch:254` |
| DeltaFIFO 结构体 | [delta_fifo.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/delta_fifo.go) | `DeltaFIFO:95` |
| controller 处理循环 | [controller.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/controller.go) | `processLoop:181` |

### Informer 四件套架构图

```
                         Kubernetes API Server
                               │  1) List & Watch
                               ▼
                          Reflector
                    (reflector.go ListAndWatch:254)
                               │  2) Add Object（Delta事件入队）
                               ▼
                        Delta FIFO Queue
                    (delta_fifo.go DeltaFIFO:95)
                     [Added/Updated/Deleted/Sync]
                               │  3) Pop Object
                               ▼
                          Informer                    4) Add Object    ┌─────────────┐
                    (shared_informer.go)  ──────────────────────────► │   Indexer    │
                    HandleDeltas()                                     │ (Thread-safe │
                               │  6) Dispatch EventHandler             │   Store)     │
                               ▼                                       └──────┬───────┘
                    Resource Event Handlers                                   │ 9) Get Object for Key
                    AddFunc / UpdateFunc / DeleteFunc                         │
                               │  7) Enqueue Object Key                      │
                               ▼                                              │
                          Workqueue                                           │
                               │  8) Get Key                                 │
                               ▼                                              │
                         Process Item  ◄─────────────────────────────────────┘
                               │
                               ▼
                         Handle Object
```

#### 图解：数据流的两条路径

收到一个 Delta 事件后，`HandleDeltas` 对每个 Delta **串行**执行两步：

- **路径 A（步骤 4-5）**：先更新 Indexer（本地缓存），保持与 etcd 一致
- **路径 B（步骤 6-7）**：再通过 `processor.distribute` 分发给注册的 EventHandler，Handler 只把 key（namespace/name）压入 Workqueue

串行而非并发的原因：确保 EventHandler 被调用时 Indexer 已是最新状态——controller 从 Workqueue 取到 key 后（步骤 8），通过步骤 9 从 Indexer 读取对象时，保证读到的是这次事件之后的最新版本。EventHandler 极轻量（只压 key），实际处理总是读最新状态，天然处理了"多个事件折叠成一次处理"的场景。

### 四件套详解

**Reflector**：负责与 apiserver 通信，内部实现 listwatch 机制。
- 先做一次 List，拿到全量资源和 `resourceVersion`
- 然后从该 `resourceVersion` 开始 Watch，接收增量变更
- 将变更封装为 Delta（类型 + 对象）写入 DeltaFIFO

**DeltaFIFO**：更新队列，FIFO 是有序队列的基本方法（ADD/UPDATE/DELETE/LIST/POP/CLOSE 等）；Delta 是资源对象存储，保存队列对象的消费类型（Added/Updated/Deleted/Sync 等）。

**Informer**：监听资源的代码抽象：
- 从 DeltaFIFO 弹出数据
- 保存到本地 Indexer 缓存（步骤 4）
- 将数据分发到自定义 controller，进行事件处理（步骤 6）

**Indexer**：Client-go 用来存储资源对象并自带索引功能的本地存储：
- Reflector 从 DeltaFIFO 消费出来的资源对象存储到 Indexer
- Indexer 与 etcd 集群中的数据完全保持一致
- 从而 client-go 可以本地读取，减少 Kubernetes API 和 etcd 集群的压力

kube-scheduler 中的具体 EventHandler 注册（`addAllEventHandlers`）和 `HandleDeltas` 的源码实现见 §05。

**注意**：每次 resync 也会触发 `UpdateFunc` 回调，即使资源本身没有变化。这是设计上的"定期对账"机制，controller 的 `UpdateFunc` 应当幂等。

---

## §05. kube-scheduler 中的 informer 源码阅读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| SharedInformerFactory 初始化 | [factory.go](kubernetes/staging/src/k8s.io/client-go/informers/factory.go) | `NewSharedInformerFactoryWithOptions:109` |
| InformerFor（按类型创建或复用） | [factory.go](kubernetes/staging/src/k8s.io/client-go/informers/factory.go) | `InformerFor:164` |
| pod-informer 创建 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `newPodInformer:666` |
| sharedIndexInformer Run | [shared_informer.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/shared_informer.go) | `Run:368` |
| controller Run（启动 Reflector） | [controller.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/controller.go) | `Run:127` |
| Indexer 创建 | [store.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/store.go) | `NewIndexer:261` |
| Indexer 底层存储 | [thread_safe_store.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/thread_safe_store.go) | `threadSafeMap:63` |
| keyFunc（namespace/name） | [store.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/store.go) | `MetaNamespaceKeyFunc:99` |
| 消费者：HandleDeltas | [shared_informer.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/shared_informer.go) | `HandleDeltas:527` |
| 事件分发给 listener | [shared_informer.go](kubernetes/staging/src/k8s.io/client-go/tools/cache/shared_informer.go) | `processorListener.run:765` |
| eventHandler 注册 | [eventhandlers.go](kubernetes/pkg/scheduler/eventhandlers.go) | `addAllEventHandlers:358` |

### 本节局部调用链

```
scheduler.New()
  │
  ▼ newPodInformer(cs, resyncPeriod)          → scheduler.go:666
  │   └── 直接构造 NewSharedIndexInformer     → 不走 factory，只监控非终态 Pod
  │
  ▼ informerFactory.Start(ctx.Done())         → server.go（Run函数）
  │   └── sharedInformerFactory.Start()       → factory.go:128
  │         └── for each informer → go informer.Run(stopCh)
  │
  ▼ informerFactory.WaitForCacheSync()        → 等待首次 List 完成，再开始调度
```

### 初始化 sharedInformerFactory

`opts.Config()` 中调用 `scheduler.NewInformerFactory(client, 0)` 创建工厂，内部委托给：

```go
// staging/src/k8s.io/client-go/informers/factory.go:109
func NewSharedInformerFactoryWithOptions(client kubernetes.Interface, defaultResync time.Duration,
    options ...SharedInformerOption) SharedInformerFactory {
    factory := &sharedInformerFactory{
        client:           client,
        namespace:        v1.NamespaceAll,
        defaultResync:    defaultResync,
        informers:        make(map[reflect.Type]cache.SharedIndexInformer),
        startedInformers: make(map[reflect.Type]bool),
        customResync:     make(map[reflect.Type]time.Duration),
    }
    for _, opt := range options {
        factory = opt(factory)
    }
    return factory
}
```

`sharedInformerFactory` 是一个 map，key 是资源类型（reflect.Type），value 是对应的 `SharedIndexInformer`。同一种资源类型只创建一个 Informer，多个组件共享，避免重复建立 Watch 连接。

### InformerFor：按反射类型复用

```go
// staging/src/k8s.io/client-go/informers/factory.go:164
func (f *sharedInformerFactory) InformerFor(obj runtime.Object, newFunc internalinterfaces.NewInformerFunc) cache.SharedIndexInformer {
    f.lock.Lock()
    defer f.lock.Unlock()

    informerType := reflect.TypeOf(obj)
    informer, exists := f.informers[informerType]
    if exists {
        return informer  // 已存在，直接复用
    }

    resyncPeriod, exists := f.customResync[informerType]
    if !exists {
        resyncPeriod = f.defaultResync
    }

    informer = newFunc(f.client, resyncPeriod)  // 首次创建
    f.informers[informerType] = informer
    return informer
}
```

`newFunc` 对于 Pod 资源是 `newPodInformer`；对于 Node 资源是另一个函数——每种资源注册自己的构造函数，`InformerFor` 负责按类型缓存和复用。

### pod-informer 的初始化

scheduler 使用的 Pod Informer 是特殊的——它不通过 `factory.Core().V1().Pods().Informer()` 获取，而是直接调用 `newPodInformer`：

```go
// pkg/scheduler/scheduler.go:666
func newPodInformer(cs clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
    selector := fmt.Sprintf("status.phase!=%v,status.phase!=%v", v1.PodSucceeded, v1.PodFailed)
    tweakListOptions := func(options *metav1.ListOptions) {
        options.FieldSelector = selector
    }
    return coreinformers.NewFilteredPodInformer(cs, metav1.NamespaceAll, resyncPeriod, nil, tweakListOptions)
}
```

这里关键的设计是 `FieldSelector`：只 Watch `status.phase != Succeeded && status.phase != Failed` 的 Pod，即只关注非终态 Pod，减少不必要的 Watch 事件。

内部通过 `listFunc` + `watchFunc` 建立对 apiserver 的 List+Watch 连接，用 `cache.NewSharedIndexInformer` 包装。

### sharedIndexInformer Run：内部结构

```
sharedIndexInformer.Run()
  │
  ├── 新建 DeltaFIFO 队列
  │     fifo := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
  │         KnownObjects: s.indexer,  // Indexer 既是已知对象的参考
  │         EmitDeltaTypeReplaced: true,
  │     })
  │
  ├── 新建 controller（config 中包含 ListerWatcher、Queue、ObjectType）
  │     s.controller = New(cfg)
  │     s.controller.(*controller).clock = s.clock
  │
  └── controller.Run(stopCh)               → controller.go:127
        ├── go r.Run(stopCh)               → 启动 Reflector（生产者）
        └── wait.Until(c.processLoop, ...)  → 消费者循环
```

`controller.Run` 中，`r.Run` 启动 Reflector，对 apiserver 做 List+Watch，把 Delta 事件放入 DeltaFIFO；`processLoop` 从 DeltaFIFO 弹出数据，调用 `HandleDeltas` 处理。

### Indexer 的创建与底层存储

```go
// staging/src/k8s.io/client-go/tools/cache/store.go:261
func NewIndexer(keyFunc KeyFunc, indexers Indexers) Indexer {
    return &cache{
        cacheStorage: NewThreadSafeStore(indexers, Indices{}),
        keyFunc:      keyFunc,
    }
}
```

底层数据结构是 `threadSafeMap`：

```go
// staging/src/k8s.io/client-go/tools/cache/thread_safe_store.go:63
type threadSafeMap struct {
    lock  sync.RWMutex
    items map[string]interface{}  // key → 具体资源对象（如 *v1.Pod）
    // indices maps a name to an Index
    indices Indices  // 二级索引：index name → index（namespace → set of keys）
}
```

两层数据结构：
- `items`：直接存储具体资源对象，key 是 `MetaNamespaceKeyFunc` 生成的 `namespace/name` 字符串
- `indices`：二级索引，加速按 namespace 等维度查找，避免全量遍历

`MetaNamespaceKeyFunc` 是传入 `NewIndexer` 的 keyFunc：

```go
// staging/src/k8s.io/client-go/tools/cache/store.go:99
func MetaNamespaceKeyFunc(obj interface{}) (string, error) {
    if key, ok := obj.(ExplicitKey); ok {
        return string(key), nil
    }
    meta, err := meta.Accessor(obj)
    if len(meta.GetNamespace()) > 0 {
        return meta.GetNamespace() + "/" + meta.GetName(), nil
    }
    return meta.GetName(), nil
}
```

有 namespace 时返回 `namespace/name`，无 namespace（如 Node）时只返回 `name`。

### 添加 eventHandler：addAllEventHandlers

scheduler 在 `New` 中调用 `addAllEventHandlers`，为 Pod、Node 等资源注册事件处理函数：

```go
// pkg/scheduler/eventhandlers.go:358
func addAllEventHandlers(sched *Scheduler, informerFactory informers.SharedInformerFactory) {
    // 已调度 pod：更新本地 cache
    informerFactory.Core().V1().Pods().Informer().AddEventHandler(
        cache.FilteringResourceEventHandler{
            FilterFunc: func(obj interface{}) bool {
                switch t := obj.(type) {
                case *v1.Pod:
                    return assignedPod(t)    // NodeName != ""，已调度
                // ...
                }
            },
            Handler: cache.ResourceEventHandlerFuncs{
                AddFunc:    sched.addPodToCache,
                UpdateFunc: sched.updatePodInCache,
                DeleteFunc: sched.deletePodFromCache,
            },
        },
    )
    // 未调度 pod：入 SchedulingQueue
    informerFactory.Core().V1().Pods().Informer().AddEventHandler(
        cache.FilteringResourceEventHandler{
            FilterFunc: func(obj interface{}) bool {
                // NodeName == "" 且属于本 scheduler 负责的 profile
                return !assignedPod(t) && responsibleForPod(t, sched.Profiles)
            },
            Handler: cache.ResourceEventHandlerFuncs{
                AddFunc:    sched.addPodToSchedulingQueue,
                UpdateFunc: sched.updatePodInSchedulingQueue,
                DeleteFunc: sched.deletePodFromSchedulingQueue,
            },
        },
    )
}
```

两个 handler 的分工：
- **已调度 Pod（NodeName != ""）**：更新本地 schedulerCache（供 Assume/调度算法查询）
- **未调度 Pod（NodeName == ""）**：加入 SchedulingQueue，等待被调度

### 启动 Informers

```go
// cmd/kube-scheduler/app/server.go（Run函数）
cc.InformerFactory.Start(ctx.Done())        // 启动所有已注册的 informer
cc.InformerFactory.WaitForCacheSync(ctx.Done())  // 等待 List 完成，缓存就绪
```

`Start` 内部遍历 `f.informers` map，对每个未启动的 informer 启动一个 goroutine 调用 `informer.Run(stopCh)`，并在 `startedInformers` map 中记录，防止重复启动。

`WaitForCacheSync` 阻塞直到所有 informer 完成首次 List（`HasSynced` 返回 true），保证调度前缓存中有完整数据。

### 消费者：HandleDeltas

```go
// staging/src/k8s.io/client-go/tools/cache/shared_informer.go:527
func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
    s.blockDeltas.Lock()
    defer s.blockDeltas.Unlock()

    for _, d := range obj.(Deltas) {
        switch d.Type {
        case Sync, Replaced, Added, Updated:
            s.cacheMutationDetector.AddObject(d.Object)
            if old, exists, err := s.indexer.Get(d.Object); err == nil && exists {
                s.indexer.Update(d.Object)  // 路径 A：更新 Indexer
                // isSync 判断是否 resync 触发
                s.processor.distribute(updateNotification{oldObj: old, newObj: d.Object}, isSync)
            } else {
                s.indexer.Add(d.Object)     // 路径 A：新增到 Indexer
                s.processor.distribute(addNotification{newObj: d.Object}, false)
            }
        case Deleted:
            s.indexer.Delete(d.Object)      // 路径 A：从 Indexer 删除
            s.processor.distribute(deleteNotification{oldObj: d.Object}, false)
        }
    }
    return nil
}
```

关键点：每次处理 Delta 时，先更新 Indexer（路径 A），再通过 `processor.distribute` 分发给所有 listener（路径 B）。两者串行执行，确保 listener 回调触发时 Indexer 已是最新状态。

`processorListener.run`（`shared_informer.go:765`）消费 listener 的通知 channel，调用注册的 `AddFunc`/`UpdateFunc`/`DeleteFunc`。





## §06. kube-scheduler 利用 informer 机制调度 pod

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 调度主循环 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `scheduleOne:441` |
| SchedulingQueue 接口 | [scheduling_queue.go](kubernetes/pkg/scheduler/internal/queue/scheduling_queue.go) | `NewSchedulingQueue:102` |
| 调度算法入口 | [generic_scheduler.go](kubernetes/pkg/scheduler/core/generic_scheduler.go) | `Schedule:97` |
| Predict 阶段（Filter） | [generic_scheduler.go](kubernetes/pkg/scheduler/core/generic_scheduler.go) | `findNodesThatFitPod:223` |
| Filter 插件执行 | [framework.go](kubernetes/pkg/scheduler/framework/runtime/framework.go) | `RunFilterPlugins:569` |
| NodeName 插件示例 | [node_name.go](kubernetes/pkg/scheduler/framework/plugins/nodename/node_name.go) | `Filter:46` |
| 插件注册表 | [registry.go](kubernetes/pkg/scheduler/framework/plugins/registry.go) | `NewInTreeRegistry:51` |
| Assume 写入本地 cache | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `assume:373` |
| AssumePod 实现 | [cache.go](kubernetes/pkg/scheduler/internal/cache/cache.go) | `AssumePod:356` |
| Bind 异步绑定 | [scheduler.go](kubernetes/pkg/scheduler/scheduler.go) | `bind:395` |
| eventHandler 注册 | [eventhandlers.go](kubernetes/pkg/scheduler/eventhandlers.go) | `addAllEventHandlers:358` |

### 本节局部调用链

```
sched.Run(ctx)                                 → OnStartedLeading 回调触发
  │
  ▼ go wait.UntilWithContext(ctx, sched.scheduleOne, 0)
  │
  ▼ scheduleOne()                              scheduler.go:441
  │   ├── sched.NextPod()                      → 从 SchedulingQueue.Pop() 取 pod
  │   ├── sched.Algorithm.Schedule()           → generic_scheduler.go:97
  │   │     ├── g.findNodesThatFitPod()        → Filter 阶段（Predict）
  │   │     │     └── RunFilterPluginsWithNominatedPods()
  │   │     │           └── RunFilterPlugins() → framework.go:569
  │   │     │                 └── pl.Filter()  → 各插件（如 NodeName:46）
  │   │     └── prioritizeNodes()              → Score 阶段（Priority）
  │   │           → 打分，返回最高分 Node
  │   │
  │   ├── sched.assume()                       → scheduler.go:373
  │   │     └── cache.AssumePod()              → 写入本地 cache，NodeName = suggestedHost
  │   │
  │   └── go sched.bind()                      → scheduler.go:395（异步执行）
  │         └── fwk.RunBindPlugins()           → 默认插件发 POST /binding 到 apiserver
```

### Pod 调度基于 SchedulingQueue 异步工作

Pod 不是被 scheduler 同步拉取的，而是由 Informer 的 EventHandler 异步推入 SchedulingQueue，再由 `scheduleOne` 消费：

```
监听到 Pod 事件（NodeName == ""）
  │
  ▼ addAllEventHandlers → AddFunc: sched.addPodToSchedulingQueue
  │
  ▼ SchedulingQueue.Add(pod)
  │   └── NewSchedulingQueue → NewPriorityQueue
  │         三个子队列：activeQ（堆）/ podBackoffQ / unschedulableQ
  │
  ▼ scheduleOne() 调用 sched.NextPod() → SchedulingQueue.Pop()
```

`SchedulingQueue` 是一个带优先级的队列（`PriorityQueue`），三个子队列：
- **activeQ**：等待调度的 Pod（按优先级排序的堆）
- **podBackoffQ**：曾经调度失败、等待退避时间的 Pod
- **unschedulableQ**：无法调度的 Pod（等待集群状态改变后重试）

### 为什么需要优先级

不同 Pod 的紧迫程度不同，`PriorityClass` 控制调度顺序：

```bash
kubectl get PriorityClass
# system-node-critical    2000001000
# system-cluster-critical 2000000000
# calico-priority         1000000000
```

高优先级 Pod（如系统组件）会优先调度，必要时可抢占低优先级 Pod 的资源。

### scheduleOne：调度主循环

```go
// pkg/scheduler/scheduler.go:441
func (sched *Scheduler) scheduleOne(ctx context.Context) {
    podInfo := sched.NextPod()
    if podInfo == nil || podInfo.Pod == nil {
        return
    }
    pod := podInfo.Pod

    // 找到对应的 scheduler profile（多调度器配置）
    fwk, err := sched.frameworkForPod(pod)
    if err != nil { return }

    // 运行调度算法（Filter + Score）
    scheduleResult, err := sched.Algorithm.Schedule(schedulingCycleCtx, sched.Extenders, fwk, state, pod)

    // Assume：写入本地 cache
    assumedPodInfo := podInfo.DeepCopy()
    assumedPod := assumedPodInfo.Pod
    err = sched.assume(assumedPod, scheduleResult.SuggestedHost)

    // Bind：异步绑定
    go func() {
        err := sched.bind(bindingCycleCtx, fwk, assumedPod, scheduleResult.SuggestedHost, state)
    }()
}
```

`assume` 和 `bind` 是有意分离的：`assume` 同步执行，立即告知本地 cache 该 Node 已被占用，防止同一 Node 被重复调度；`bind` 异步执行，发网络请求到 apiserver 不阻塞主循环，调度下一个 Pod 时本地 cache 已感知该假设。

### Schedule 调度算法解析

```go
// pkg/scheduler/core/generic_scheduler.go:97
func (g *genericScheduler) Schedule(ctx context.Context, fwk framework.Framework,
    state *framework.CycleState, pod *v1.Pod) (result ScheduleResult, err error) {

    // 快照当前 Node 信息（每次调度前同步一次 cache 快照）
    if err := g.snapshot(); err != nil { return result, err }

    if g.nodeInfoSnapshot.NumNodes() == 0 {
        return result, ErrNoNodesAvailable
    }

    // Predict 阶段：Filter
    feasibleNodes, diagnosis, err := g.findNodesThatFitPod(ctx, extenders, fwk, state, pod)

    if len(feasibleNodes) == 0 {
        return result, &framework.FitError{Pod: pod, NumAllNodes: g.nodeInfoSnapshot.NumNodes(), Diagnosis: diagnosis}
    }

    // Priority 阶段：Score（只有一个节点时跳过打分，直接选它）
    priorityList, err := prioritizeNodes(ctx, extenders, fwk, state, pod, feasibleNodes)
    host, err := selectHost(priorityList)
    return ScheduleResult{SuggestedHost: host, ...}, err
}
```

### Predict 与 Priority

**Predict（Filter）阶段**：
- 遍历所有节点，调用 Filter 插件判断节点是否满足 Pod 的调度要求
- 满足的节点放入 `feasibleNodes`，不满足的直接过滤
- 如果 `feasibleNodes` 为空，返回调度失败，记录 `FailedScheduling` Event

**Priority（Score）阶段**：
- 对 `feasibleNodes` 中的每个节点调用 Score 插件打分
- 累加各插件分数，选出总分最高的节点作为调度目标

每个插件的执行通过 framework 调用：

```go
// pkg/scheduler/framework/runtime/framework.go:569
func (f *frameworkImpl) RunFilterPlugins(ctx context.Context, state *framework.CycleState,
    pod *v1.Pod, nodeInfo *framework.NodeInfo) framework.PluginToStatus {
    for _, pl := range f.filterPlugins {
        if f.ShouldRecordPluginMetrics() { /* 记录延迟 */ }
        status = pl.Filter(ctx, state, pod, nodeInfo)  // 调用每个 Filter 插件
        if !status.IsSuccess() {
            statuses[pl.Name()] = status
        }
    }
}
```

### 以 NodeName 插件为例解读一个 Filter

`NodeName` 插件检查 Pod spec 中是否指定了 `nodeName`，如果指定了则只有名字匹配的 Node 通过过滤：

```go
// pkg/scheduler/framework/plugins/nodename/node_name.go:46
func (pl *NodeName) Filter(ctx context.Context, _ *framework.CycleState, pod *v1.Pod,
    nodeInfo *framework.NodeInfo) *framework.Status {
    if nodeInfo.Node() == nil {
        return framework.NewStatus(framework.Error, "node not found")
    }
    if !Fits(pod, nodeInfo) {
        return framework.NewStatus(framework.UnschedulableAndUnresolvable,
            ErrReason)
    }
    return nil
}

func Fits(pod *v1.Pod, nodeInfo *framework.NodeInfo) bool {
    return len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName == nodeInfo.Node().Name
}
```

插件的 `New` 函数在启动时通过 registry 注册：

```go
// pkg/scheduler/framework/plugins/registry.go:51
func NewInTreeRegistry() runtime.Registry {
    fts := plfeature.Features{...}
    return runtime.Registry{
        selectorspread.Name:           selectorspread.New,
        imagelocalitypriority.Name:    imagelocalitypriority.New,
        tainttoleration.Name:          tainttoleration.New,
        nodename.Name:                 nodename.New,  // ← NodeName 注册在此
        // ... 几十个内置插件
    }
}
```

### Assume 验证：写入本地 cache

```go
// pkg/scheduler/scheduler.go:373
func (sched *Scheduler) assume(assumed *v1.Pod, host string) error {
    assumed.Spec.NodeName = host  // 直接修改 Pod 对象的 NodeName
    if err := sched.Cache.AssumePod(assumed); err != nil {
        klog.Errorf("scheduler cache AssumePod failed: %v", err)
        return err
    }
    if sched.SchedulingQueue != nil {
        sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
    }
    return nil
}
```

`AssumePod`（`cache.go:356`）：
- 根据 Pod uid 在 cache 中查找，正常情况找不到（新 Pod）
- 将 Pod 写入 `cache.podStates[key]`
- 把 key 写入 `cache.assumedPods`（标记为假设状态）
- `addPod` 把 Pod 信息累加到对应 `nodeInfo`（如更新 Node 的 requested CPU/Memory）

这样后续调度同一 Node 时，算法能看到该 Node 已被该 Pod "占用"的资源，防止超量调度。

### bind 操作解读

```go
// pkg/scheduler/scheduler.go:395
func (sched *Scheduler) bind(ctx context.Context, fwk framework.Framework,
    assumed *v1.Pod, targetNode string, state *framework.CycleState) (err error) {
    defer func() {
        sched.finishBinding(fwk, assumed, targetNode, start, err)
    }()
    bound, err := sched.extendersBinding(assumed, targetNode)  // 先尝试 extender binding
    if bound { return err }
    bindStatus := fwk.RunBindPlugins(ctx, state, assumed, targetNode)
    if bindStatus.IsSuccess() { return nil }
    return fmt.Errorf("bind status: %s, %v", bindStatus.Code().String(), bindStatus.Message())
}
```

默认 Bind 插件向 apiserver 发 POST 请求，创建 Binding 对象：

```go
// 底层请求（extender 绑定路径）
func (h *HTTPExtender) Bind(binding *v1.Binding) error {
    req := &extenderv1.ExtenderBindingArgs{
        PodName:      binding.Name,
        PodNamespace: binding.Namespace,
        PodUID:       binding.UID,
        Node:         binding.Target.Name,
    }
    h.send(h.bindVerb, req, &result)
}
```

apiserver 收到 Binding 后，将 Pod 的 `NodeName` 写入 etcd，kubelet 随后 Watch 到该 Pod 分配给自己的 Node，开始拉取镜像并启动容器。

