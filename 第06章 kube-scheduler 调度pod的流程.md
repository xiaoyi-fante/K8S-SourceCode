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

5. **Informer 四件套：Reflector + DeltaFIFO + Informer + Indexer**：Reflector 对 apiserver 做 List+Watch，变更事件写入 DeltaFIFO 队列；Informer 从队列弹出，一路同步到 Indexer（本地缓存），同时分发到注册的 EventHandler 回调；Indexer 提供 O(1) 的本地读取，彻底解耦 controller 与 apiserver/etcd 的直接通信。

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

### 为什么需要 Informer

如果每个 controller 都直接对 apiserver/etcd 发 List+Watch 请求，随着组件数量增加，etcd 连接数和查询压力会成倍增长。Informer 机制的核心价值：

- **实时性**：Watch 而非轮询，事件到达即处理
- **可靠性**：内置 resync 机制，定期全量同步防止事件丢失
- **顺序性**：DeltaFIFO 按 key 保证事件顺序处理
- **解耦**：本地 Indexer 缓存让 controller 的读操作完全不走网络

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

收到一个 Delta 事件后，Informer 同时做两件事（步骤 4 和步骤 6 是并发的）：

- **路径 A（步骤 4-5）**：将最新资源对象存入 Indexer（本地缓存），保持与 etcd 一致
- **路径 B（步骤 6-7）**：将事件分发给注册的 EventHandler，Handler 只把 key（namespace/name）压入 Workqueue，不做实际处理

controller 的 `Process Item`（步骤 8）从 Workqueue 取到 key 后，通过步骤 9 从 Indexer 读取最新对象（不走网络），再执行业务逻辑。这个设计的好处是：EventHandler 极轻量（只压 key），实际处理总是读最新状态，天然处理了"多个事件折叠成一次处理"的场景。

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

### 使用 informer 代码示例

以监听 Pod 变化为例（k8s-informer 项目）：

```go
// 1. 创建 SharedInformerFactory（resync 周期 = 1分钟）
sharedInformers := informers.NewSharedInformerFactory(clientset, time.Minute)

// 2. 获取 Pod 资源的 Informer
informer := sharedInformers.Core().V1().Pods().Informer()

// 3. 注册 EventHandler（只压 key，不做实际处理）
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        mObj := obj.(v1.Object)
        log.Printf("New Pod Added to Store: %s", mObj.GetName())
    },
    UpdateFunc: func(oldObj, newObj interface{}) {
        oObj := oldObj.(v1.Object)
        nObj := newObj.(v1.Object)
        log.Printf("%s Pod Updated to %s", oObj.GetName(), nObj.GetName())
    },
    DeleteFunc: func(obj interface{}) {
        mObj := obj.(v1.Object)
        log.Printf("Pod Deleted from Store: %s", mObj.GetName())
    },
})

// 4. 启动 Informer（内部启动 Reflector 开始 List+Watch）
informer.Run(ctx.Done())
```

**注意**：每次 resync 也会触发 `UpdateFunc` 回调，即使资源本身没有变化。这是设计上的"定期对账"机制，controller 的 `UpdateFunc` 应当幂等。

运行效果：启动后立即打印集群内所有 Pod（List 阶段），之后新建/更新/删除 Pod 时实时触发对应回调。

---

## 05. kube-scheduler 中的 informer 源码阅读

## 06. kube-scheduler 利用 informer 机制调度 pod
