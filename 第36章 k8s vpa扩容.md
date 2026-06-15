# 第36章 k8s VPA 扩容

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 36 章 — k8s vpa 扩容
> **源码入口**: `vertical-pod-autoscaler/pkg/recommender/main.go`（VPA 独立项目，非 k8s 主库）

---

## 核心机制一览

1. **VPA vs HPA 的本质区别**：HPA 横向调整副本数，VPA 纵向调整单个 Pod 的 CPU/内存 request/limit。两者**不能同时使用**（VPA 驱逐 Pod 重建时会与 HPA 的副本数决策冲突）。

2. **三组件分工**：VPA 由三个独立进程组成——**Recommender**（收集指标、计算推荐值、写入 VPA status）、**Updater**（监听 VPA，驱逐需要更新的 Pod）、**AdmissionController**（拦截 Pod 创建请求，按推荐值修改 resource request）。三者通过 VPA CRD 和 etcd 解耦，互不直接调用。

3. **Recommender 的 RunOnce 五步**：LoadVPAs → LoadPods → LoadRealTimeMetrics → UpdateVPAs → MaintainCheckpoints。核心是维护一个内存中的 `clusterState`，汇聚所有 VPA/Pod/容器指标后，用百分位算法计算推荐值并写回 VPA CRD。

4. **Updater 驱逐的本质**：向 kube-apiserver 发 POST 请求调用 Pod 的 eviction 子资源（等同于 `kubectl drain`），将 podPhase 写为 Failed 触发重新调度。Pod 重建时经过 AdmissionController，获得新的 resource 值。

5. **AdmissionController 自注册 Webhook**：启动时调用 API 自动创建 `MutatingWebhookConfiguration`（监听 pods + verticalpodautoscalers 两类资源），不需要手动写 YAML。核心逻辑在 `Serve()` → `admit()` → `GetPatches()` → `CalculatePatches()`，最终返回 JSON Patch 修改 Pod 的 container resource request。

6. **Checkpoint 持久化历史数据**：Recommender 将指标直方图以 `VerticalPodAutoscalerCheckpoints` CRD 形式存入 etcd，重启后可从 checkpoint 恢复历史数据，避免冷启动推荐值不准。

---

## 全章调用链总图

```
                         ┌─────────────────────────────┐
                         │       Recommender            │
                         │  RunOnce() 每 metricsFetcherInterval │
                         │  ① LoadVPAs                  │
                         │  ② LoadPods                  │
                         │  ③ LoadRealTimeMetrics        │──→ metrics-server
                         │  ④ UpdateVPAs (写推荐值)      │
                         │  ⑤ MaintainCheckpoints        │──→ etcd (VPA Checkpoint CRD)
                         └──────────┬──────────────────┘
                                    │ 写 VPA.status.recommendation
                                    ▼
                         ┌─────────────────────────────┐
                         │  VPA CRD (etcd)              │
                         │  status.recommendation:      │
                         │    lowerBound / target /     │
                         │    upperBound                │
                         └──────┬──────────┬───────────┘
                                │          │
              ┌─────────────────▼──┐  ┌────▼──────────────────────┐
              │      Updater        │  │   AdmissionController     │
              │ RunOnce() 每        │  │   HTTPS Webhook Server    │
              │ updaterInterval     │  │   监听 /                  │
              │ ① List VPAs        │  │   ① GetMatchingVPA         │
              │ ② List Pods        │  │   ② CalculatePatches       │
              │ ③ getPodUpdateOrder │  │   ③ 返回 JSON Patch        │
              │ ④ evict Pod        │  └───────────────────────────┘
              └─────────┬──────────┘          ▲
                        │ POST eviction        │ Pod CREATE 请求
                        ▼                      │
              Pod 重调度 ──────────────────────┘
              (kube-scheduler → kubelet → 新 Pod 经 AdmissionController)
```

---

## §01 安装 VPA 控制器并使用

### 部署 VPA

```bash
# clone VPA 项目
git clone https://github.com/kubernetes/autoscaler.git
cd autoscaler/vertical-pod-autoscaler

# 部署三个组件（recommender + updater + admission-controller）
./hack/vpa-up.sh
```

部署后在 kube-system namespace 下可见三个 Deployment：`vpa-recommender`、`vpa-updater`、`vpa-admission-controller`。

### 创建测试 Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vpa-nginx-off01
  namespace: vpa
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vpa-nginx-off01
  template:
    metadata:
      labels:
        app: vpa-nginx-off01
    spec:
      containers:
      - name: nginx
        image: nginx
        resources:
          requests:
            cpu: 100m
            memory: 50Mi
---
apiVersion: v1
kind: Service
metadata:
  name: vpa-nginx-off01
  namespace: vpa
spec:
  ports:
  - port: 80
    targetPort: 80
  selector:
    app: vpa-nginx-off01
```

```bash
kubectl create ns vpa
kubectl apply -f nginx-deploy.yaml -n vpa
kubectl get all -n vpa
```

### 创建 VPA 对象（updateMode: Off）

```yaml
apiVersion: autoscaling.k8s.io/v1beta2
kind: VerticalPodAutoscaler
metadata:
  name: vpa-nginx-off01
  namespace: vpa
spec:
  targetRef:
    apiVersion: "apps/v1"
    kind: Deployment
    name: vpa-nginx-off01
  updatePolicy:
    updateMode: "Off"        # 只给推荐值，不实际更新 Pod
  resourcePolicy:
    containerPolicies:
    - containerName: "nginx"
      minAllowed:
        cpu: "250m"
        memory: "100Mi"
      maxAllowed:
        cpu: "2000m"
        memory: "2048Mi"
```

**updateMode 四种取值：**

| updateMode | 行为 |
|-----------|------|
| `Off` | 只计算推荐值写入 VPA status，不驱逐 Pod |
| `Initial` | 只在 Pod 创建时应用推荐值，运行中不驱逐 |
| `Recreate` | 运行中超出范围时驱逐 Pod 并重建 |
| `Auto` | 等同 Recreate（当前实现），未来可能支持原地更新 |

### 验证推荐值（updateMode: Off）

```bash
kubectl describe vpa vpa-nginx-off01 -n vpa
```

关键字段解读：

| 字段 | 含义 |
|------|------|
| `Lower Bound` | 下限值，VPA 给出的最小资源边界 |
| `Target` | **推荐值**，VPA 认为最合适的资源量 |
| `Upper Bound` | 上限值，VPA 给出的最大资源边界 |
| `Uncapped Target` | 如果没有为 VPA 提供最小或最大边界，则表示目标利用率 |

用 ab 压测（10000000 总数 100 并发）：

```bash
yum -y install httpd-tools
kubectl get svc -n vpa   # 获取 ClusterIP
ab -c 100 -n 10000000 http://10.96.12.138/
```

压测后 VPA 给出推荐值：CPU: 1038m，由于 updateMode: "Off" 不会更新 Pod。

### 体验 updateMode: "Auto"

将 updateMode 改为 `Auto` 后重新压测：

```bash
kubectl edit vpa vpa-nginx-off01 -n vpa
# updateMode: "Auto"

# 再次压测
ab -c 100 -n 10000000 http://10.96.12.138/

# 监控 Pod 和 event
kubectl get pod -n vpa
kubectl get event -n vpa -w
```

几分钟后观察到驱逐事件：

```
0s  Normal  EvictedByVPA     pod/vpa-nginx-off01-79f78f6797-7xnt5  Pod was evicted !
0s  Normal  Killing          pod/vpa-nginx-off01-79f78f6797-7xnt5  Stopping container ...
0s  Normal  SuccessfulCreate replicaset/...                         Created pod: ...
0s  Normal  Scheduled        pod/vpa-nginx-off01-79f78f6797-phcs   ...
0s  Normal  Pulled           pod/vpa-nginx-off01-79f78f6797-phcs   ...
0s  Normal  Started          pod/vpa-nginx-off01-79f78f6797-phcs   ...
```

新 Pod 的 resource request 已被 VPA 设置为推荐值：CPU: 250m，Memory: 262144k。

### VPA 的限制

- **不能与 HPA 同时使用**：两者在副本数和资源请求上会产生冲突
- **副本数为 1 时不能驱逐**：`too less pod`，驱逐会导致服务中断

---

## §02 Recommender 源码解读

| 读码目标 | 源文件 | 入口函数 |
|---------|--------|---------|
| main 入口 | `pkg/recommender/main.go` | `main` |
| 初始化 Recommender | `pkg/recommender/routines/recommender.go` | `NewRecommender` |
| ClusterState 结构 | `pkg/recommender/model/cluster.go` | `NewClusterState` |
| ClusterStateFeeder | `pkg/recommender/input/cluster_feeder.go` | `NewClusterStateFeeader` |
| ControllerFetcher | `pkg/recommender/input/controller_fetcher/controller_fetcher.go` | `NewControllerFetcher` |
| 推荐值计算 | `pkg/recommender/logic/recommender.go` | `GetRecommendedPodResources` |
| RunOnce 主循环 | `pkg/recommender/routines/recommender.go` | `RunOnce` |

### 架构图

```
main()
  │
  ▼ model.InitializeAggregationsConfig   (配置内存聚合间隔等参数)
  │
  ▼ routines.NewRecommender(config, checkpointInterval, useCheckpoints, ...)
  │  ├── NewClusterStateFeeder           (数据输入层)
  │  │    ├── NewControllerFetcher       (找 topmost scalable controller)
  │  │    ├── NewVpaLister               (list/watch VPA 对象)
  │  │    └── NewVpaTargetSelectorFetcher (list/watch scale 对象的 informers)
  │  └── NewPodResourceRecommender       (推荐算法层)
  │
  ▼ ticker := time.Tick(*metricsFetcherInterval)
     for range ticker {
         recommender.RunOnce()           (每 tick 执行一次)
     }
```

### 01 ClusterState — VPA 对象的内存模型

```go
// ClusterState 是整个 recommender 的核心数据结构
type ClusterState struct {
    Pods      map[PodID]*PodState         // 所有被 VPA 管控的 Pod
    Vpas      map[VpaID]*Vpa              // 所有 VPA 对象
    EmptyVpas map[VpaID]time.Time         // 尚无推荐值映射的 VPA，记录首次出现时间（用于发出警告）
    ObservedVpas []*vpa_types.VerticalPodAutoscaler
    aggregateStateMap map[aggregateStateKey]*AggregateContainerState
    labelSetMap map[labelSetKey]labels.Set
    LastAggregateContainerStateGC time.Time
    Time interval.Time
}
```

### 02 ClusterStateFeeder — 使用来更新 ClusterState

```go
// NewClusterStateFeeder 构造时创建并启动众多 scale 对象的 informer
// 支持的 scale 目标类型：
//   DaemonSets / Deployment / ReplicaSet / StatefulSet / ReplicationController / Job / CronJob
func NewClusterStateFeeder(config *rest.Config, clusterState *model.ClusterState, ...) ClusterStateFeeder {
    // ...
    // discoveryClient 发现 API，resolverClient 获取 informer
    // informersMap: 按 kind 分类的 SharedIndexInformer
    for kind, informer := range informersMap {
        stopCh := make(chan struct{})
        go informer.Run(stopCh)
        synced := cache.WaitForCacheSync(stopCh, informer.HasSynced)
        // ...
    }
    scaleNamespacer := scale.New(restClient, mapper, ...)
    return &controllerFetcher{
        scaleNamespacer: scaleNamespacer,
        mapper:          mapper,
        informersMap:    informersMap,
        scaleSubresourceCacheStorage: newControllerCacheStorage(...),
    }
}
```

`controllerFetcher` 通过 `periodicallyRefreshCache` 定期刷新各 namespace 下的 scale 对象缓存，`controllerCacheStorage.Refresh` 负责更新每条缓存条目的 `refreshAfter`/`deleteAfter` 时间戳。

**VPA Lister** 初始化：

```go
VpaLister: vpa_api_util.NewVpaLister(vpa_clientset.NewForConfigOrDie(config), make(chan struct{}), namespace)

func NewVpaLister(vpaClient *vpa_clientset.Clientset, stopChannel <-chan struct{}, namespace string) v1.VerticalPodAutoscalerLister {
    vpaListWatch := cache.NewListWatchFromClient(vpaClient.AutoscalingV1().RESTClient(), "verticalpodautoscalers", ...)
    indexer, controller := cache.NewIndexerInformer(vpaListWatch,
        &vpa_types.VerticalPodAutoscaler{},
        1*time.Hour,
        &cache.ResourceEventHandlerFuncs{},
        cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
    vpaLister := vpa_lister.NewVerticalPodAutoscalerLister(indexer)
    go controller.Run(stopChannel)
    // ...
    return vpaLister
}
```

### 03 PodResourceRecommender — 计算推荐值

```go
// GetRecommendedPodResources 为每个 VPA 下的 Pod 计算 cpu/mem 推荐值
func (r *podResourceRecommender) GetRecommendedPodResources(containerNameToAggregateStateMap ...) RecommendedPodResources {
    // targetPctile    = 0.9   → Target（推荐值）
    // lowerBoundPctile = 0.5  → Lower Bound（下限）
    // upperBoundPctile = 0.95 → Upper Bound（上限）
}
```

**置信度乘数逻辑（根据历史长度动态调整）：**

- **upper bound 乘数**：历史短则乘数大（更保守地高估上限），确保 updater 不会过早缩减资源。公式：`altchConfidenceMultiplier(1.0, 1.0, upperBoundEstimator)`，history 越短指数越大（最高 +1e6 倍）。
- **lower bound 乘数**：历史短则乘数小（更保守地低估下限），确保分配的资源不低于实际需要。公式类似，但方向相反。

### 04 RunOnce — 五步主循环

```go
func (r *recommender) RunOnce() {
    timer := metrics_recommender.NewExecutionTimer()
    ctx := context.WithDeadline(context.Background(), time.Now().Add(*checkpointsWriteTimeout))

    // 01 LoadVPAs：从 vpaLister 获取所有 VPA，同步到 clusterState
    r.clusterStateFeeder.LoadVPAs()
    // → 遍历 vpaCRDs，AddOrUpdateVpa / clusterState.DeleteVpa
    // → 同步 clusterState.ObservedVpas = vpaCRDs

    // 02 LoadPods：更新 Pod 状态
    r.clusterStateFeeder.LoadPods()
    // → specClient.GetPodSpecs() 获取最新 pod 状态
    // → 不存在则 DeletePod，存在则 AddOrUpdatePod + AddOrUpdateContainer

    // 03 LoadRealTimeMetrics：从 metricsClient 获取当前资源用量
    r.clusterStateFeeder.LoadRealTimeMetrics()
    // → metricsClient.GetContainerMetrics() (同 kubectl top 的数据源)
    // → 遍历 containerMetrics → newContainerUsageSamplesWithKey
    // → clusterState.AddSample(sample)
    // OOM 处理：
    // select { case oomInfo := <-feeder.oomChan:
    //     clusterState.RecordOOM(oomInfo) }
    // RecordOOM：memory peak + 100M 后 × 1.2 作为 OOM 后的人工内存样本

    // 04 UpdateVPAs：计算推荐值并写回 VPA CRD
    r.UpdateVPAs()
    // → for observedVpa in clusterState.ObservedVpas:
    //     resources := r.podResourceRecommender.GetRecommendedPodResources(...)
    //     vpa.UpdateRecommendation(getCappedVpa(vpa.ID, resources, observedVpa))
    //     vpa_utils.UpdateVpaStatusIfNeeded(vpaClient.VerticalPodAutoscalers(...), vpa)

    // 05 MaintainCheckpoints：将历史数据持久化到 etcd
    r.MaintainCheckpoints(ctx, *minCheckpointsPerRun)
    // → if r.useCheckpoints: r.checkpointWriter.StoreCheckpoints(ctx, ...)
    //   每次 GC 过期 checkpoint
}
```

### Checkpoint 机制

`VerticalPodAutoscalerCheckpoints` 是一种 CRD，存储在 etcd 中，保存 VPA 对应的历史监控数据直方图：

```bash
kubectl get VerticalPodAutoscalerCheckpoints -n vpa
# NAME                      AGE
# vpa-nginx-off01-nginx     4h30m

kubectl get VerticalPodAutoscalerCheckpoints -n vpa -o yaml
```

```yaml
spec:
  containerName: nginx
  vpaObjectName: vpa-nginx-off01
status:
  cpuHistogram:
    bucketWeights:
      "0": 10000
      "13": 18
      "14": 20
      "25": 35
    referenceTimestamp: "2021-11-04T00:00:00Z"
    totalWeight: 71.22690875829201
  memoryHistogram:
    bucketWeights:
      "0": 10000
    referenceTimestamp: "2021-11-05T00:00:00Z"
    totalWeight: 10.256443489574725
```

`useCheckpoints` 代表从本地 Vpa checkpoint 加载数据，feeder 的 `InitFromCheckpoints` 遍历所有 namespace 下的 VPA，逐一读取 checkpoint 并写入 clusterState。

如果是 prometheus 模式，则从 prometheus 查询 8 天的历史数据：

```go
config := history.PrometheusHistoryProviderConfig{
    Address:                *prometheusAddress,
    QueryTimeout:           promQueryTimeout,
    HistoryLength:          *historyLength,
    HistoryResolution:      *historyResolution,
    PodLabelPrefix:         *podLabelPrefix,
    PodLabelsMetricName:    *podLabelsMetricName,
    // ...
}
provider, err = history.NewPrometheusHistoryProvider(config)
// 在 InitFromHistoryProvider 中，调用 GetClusterHistory 获取历史数据
// prometheusClient.QueryRange(ctx, query, prometheusv1.Range{Start, End, Step})
```

---

## §03 Updater 源码解读

| 读码目标 | 源文件 | 入口函数 |
|---------|--------|---------|
| main 入口 | `pkg/updater/main.go` | `main` |
| 构造 Updater | `pkg/updater/logic/updater.go` | `NewUpdater` |
| 主循环 | `pkg/updater/logic/updater.go` | `RunOnce` |
| 驱逐 Pod | `pkg/updater/eviction/pods_eviction_restriction.go` | `Evict` |

### Updater 做了什么

通过 VPA informer 监听 informer 的变化，找到对应的 Pod 执行驱逐动作。驱逐的底层就是把 `podPhase=Failed` 更新到 etcd 中，然后让 Pod 重新调度，之后通过 admission-controller 更改 Pod 的 request 值。

### 初始化

```go
// main.go
targetSelectorFetcher := target.NewVpaTargetSelectorFetcher(config, kubeClient, factory)
// NewVpaTargetSelectorFetcher 与 Recommender 中的完全相同：
// 为 Deployment/DaemonSet/ReplicaSet/StatefulSet/ReplicationController/Job/CronJob 启动 informer

// 构造 updater 对象
updater, err := updater.NewUpdater(
    kubeClient,
    vpaClient,
    *minReplicas,
    *evictionRateLimit,
    *evictionRateBurst,
    *evictionToleranceFraction,
    *useAdmissionControllerStatus,
    admissionControllerStatusNamespace,
    vpa_api_util.NewCappingRecommendationProcessor(limitRangeCalculator),
    nil,
    targetSelectorFetcher,
    priority.NewProcessor(),
    *vpaObjectNamespace,
)

// 周期执行
ticker := time.Tick(*updaterInterval)
for range ticker {
    ctx := context.WithTimeout(context.Background(), *updaterInterval)
    updater.RunOnce(ctx)
}
```

### RunOnce 解读

```
RunOnce(ctx)
  │
  ▼ 检查 AdmissionController 状态（useAdmissionControllerStatus 开启时）
  │  statusValidator.IsStatusValid → 超时则跳过本轮驱逐
  │
  ▼ vpaLister.List → 所有 VPA 列表
  │
  ▼ 过滤 updateMode ≠ Recreate 且 ≠ Auto 的 VPA（跳过 Off/Initial）
  │
  ▼ for vpa: selectorFetcher.Fetch → 获取 selector
  │   vpas = append(vpas, VpaWithSelector{vpa, selector})
  │
  ▼ podLister.List → 所有 Pod，filterDeletedPods 过滤已删除的
  │
  ▼ 构造 controlledPods map[vpa][]pod
  │   for pod: GetControllingVPAForPod(pod, vpas) → controlledPods[vpa] += pod
  │
  ▼ for vpa, livePods in controlledPods:
  │   evictionLimiter := NewPodsEvictionRestriction(livePods)
  │   podsForUpdate := getPodUpdateOrder(filterNonEvictablePods(livePods, evictionLimiter))
  │
  ▼ for pod in podsForUpdate:
      if !evictionLimiter.CanEvict(pod) → continue
      evictionRateLimiter.Wait(ctx)
      evictionLimiter.Evict(pod, eventRecorder)
```

**驱逐底层实现：**

```go
// podsEvictionRestrictionImpl.Evict
func (e *podsEvictionRestrictionImpl) Evict(podToEvict *apiv1.Pod, eventRecorder record.EventRecorder) error {
    err := e.client.CoreV1().Pods(podToEvict.Namespace).Evict(context.TODO(), eviction)
}

// pods.Evict
func (c *pods) Evict(ctx context.Context, eviction *policy.Eviction) error {
    return c.client.Post().Namespace(c.ns).Resource("pods").Name(eviction.Name).SubResource("eviction")...
}
```

这是标准的 Kubernetes Eviction API，底层将 pod.Status.Phase 设为 Failed，触发 kube-scheduler 重新调度该 Pod。

---

## §04 AdmissionController 源码解读

| 读码目标 | 源文件 | 入口函数 |
|---------|--------|---------|
| main 入口 | `pkg/admission-controller/main.go` | `main` |
| HTTP handler | `pkg/admission-controller/logic/server.go` | `AdmissionServer.Serve` |
| admit 逻辑 | `pkg/admission-controller/logic/server.go` | `admit` |
| Pod patch 生成 | `pkg/admission-controller/resource/pod/handler.go` | `GetPatches` |
| 资源 patch 计算 | `pkg/admission-controller/resource/pod/recommendation/` | `CalculatePatches` |
| 自注册 Webhook | `pkg/admission-controller/logic/server.go` | `selfRegistration` |

### AdmissionController 做了什么

通过 HTTPS server 处理 Pod 的 admit 请求：首先调用 `GetMatchingVPA` 获取 Pod 对应的 VPA，最后通过 `CalculatePatches` 生成 Pod 的 partialPatches，对应就是根据 VPA 的 Recommend 值修改 Pod 的 request 值。**注意：VPA 默认没有缩容方法**，只会提升资源，不会降低。

### 初始化

```go
// 与 Recommender/Updater 相同，先构造 targetSelectorFetcher 和 informers
targetSelectorFetcher := target.NewVpaTargetSelectorFetcher(config, kubeClient, factory)

// 推荐值提供者和 VPA 匹配器
recommendationProvider := recommendation.NewProvider(limitRangeCalculator, vpa_api_util.NewVpaForPod)
vpaMatcher := vpa.NewMatcher(vpaLister, targetSelectorFetcher)

// statusUpdater 定期更新自己的 Lease（防止单点故障检测）
statusUpdater := status.NewUpdater(kubeClient, status.AdmissionControllerStatusName, statusNamespace, statusUpdateInterval, hostname)
```

部署后可以看到 Lease 对象（HA 健康检测用）：

```bash
kubectl get lease -A | grep vpa-admission-controller
# kube-system   vpa-admission-controller   vpa-admission-controller-6bbd694ccb-spt47
```

### HTTPS Server 与 Webhook 自注册

```go
// 创建 handler 并启动 HTTPS server
calculators := []patch.Calculator{patch.NewResourceUpdatesCalculator(recommendationProvider, ...)}
as := logic.NewAdmissionServer(podPreprocessor, vpaPreprocessor, limitRangeCalculator, vpaMatcher, ...)
http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    as.Serve(w, r)
    healthCheck.UpdateLastActivity()
})
server := &http.Server{
    Addr:      fmt.Sprintf(":%d", *port),
    TLSConfig: configTLS(clientset, certs.serverCert, certs.serverKey),
}

// 启动时自注册 MutatingWebhookConfiguration
go func() {
    selfRegistration(clientset, certs.caCert, namespace, *serviceName, url, *registerByURL, int32(*webhookTimeout))
    statusUpdater.Run(stopCh)
}()
server.ListenAndServeTLS("", "")
```

`selfRegistration` 调用 `client.AdmissionregistrationV1().MutatingWebhookConfigurations()` 自动创建 Webhook，监听两类资源：

```go
webhookConfig := &admissionregistration.MutatingWebhookConfiguration{
    Webhooks: []admissionregistration.MutatingWebhook{{
        Name: "vpa.k8s.io",
        Rules: []admissionregistration.RuleWithOperations{
            {   // 拦截 Pod 操作
                Rule: admissionregistration.Rule{
                    APIGroups:   []string{""},
                    APIVersions: []string{"v1"},
                    Resources:   []string{"pods"},
                },
            },
            {   // 拦截 VPA 操作
                Rule: admissionregistration.Rule{
                    APIGroups:   []string{"autoscaling.k8s.io"},
                    APIVersions: []string{"*"},
                    Resources:   []string{"verticalpodautoscalers"},
                },
            },
        },
        FailurePolicy: &failurePolicy,        // Ignore
        SideEffects:   &sideEffects,          // None
        ClientConfig:  RegisterClientConfig,  // CABundle 来自证书
    }},
}
```

与第 04 章手动写 MutatingWebhookConfiguration YAML 不同，VPA 的 admission-controller 在启动时通过代码自动注册，无需用户手动维护 YAML。

### Serve → admit → GetPatches 调用链

```
AdmissionServer.Serve(w, r)
  │
  ▼ ioutil.ReadAll(r.Body)
  │  verify Content-Type == "application/json"
  │
  ▼ s.admit(body) → reviewResponse
  │
  ▼ json.Marshal(v1beta1.AdmissionReview{Response: reviewResponse})
     w.Write(resp)
```

**admit() 解析：**

```go
func (s *AdmissionServer) admit(data []byte) *v1beta1.AdmissionResponse {
    // 反序列化 AdmissionReview
    ar := v1beta1.AdmissionReview{}
    json.Unmarshal(data, &ar)

    // 根据 GroupResource 找对应的 handler
    admittedGroupResource := metav1.GroupResource{
        Group:    ar.Request.Resource.Group,
        Resource: ar.Request.Resource.Resource,
    }
    handler, ok := s.resourceHandlers[admittedGroupResource]

    // 调用 handler 生成 patches
    patches, err = handler.GetPatches(ar.Request)
    resource = handler.AdmissionResource()
    // ...
}
```

**pod GetPatches（核心逻辑）：**

```go
func (h *resourceHandler) GetPatches(ar *v1beta1.AdmissionRequest) ([]resource.PatchRecord, error) {
    // 1. 反序列化 Pod
    pod := v1.Pod{}
    json.Unmarshal(ar.Object.Raw, &pod)

    // 2. 找到控制该 Pod 的 VPA
    controllingVpa := h.vpaMatcher.GetMatchingVPA(&pod)
    if len(pod.Name) == 0 {
        pod.Name = pod.GenerateName + "$"   // generateName 场景
    }
    klog.V(4).Infof("Admitting pod %v", pod.ObjectMeta)

    // 3. CalculatePatches：根据 VPA 推荐值生成 JSON Patch
    var patches []resource.PatchRecord
    if pod.Annotations == nil {
        patches = append(patches, patch.GetAddEmptyAnnotationsPatch())
    }
    for _, c := range h.patchCalculators {
        partialPatches, err := c.CalculatePatches(&pod, controllingVpa)
        patches = append(patches, partialPatches...)
    }
    return patches, nil
}
```

**最终返回 AdmissionResponse：**

```go
if len(patches) > 0 {
    patch, _ := json.Marshal(patches)
    patchType := v1beta1.PatchTypeJSONPatch
    response.PatchType = &patchType
    response.Patch = patch
    klog.V(4).Infof("Sending patches: %v", patches)
}

// 设置 status
if len(patches) > 0 {
    status = metrics_admission.Applied
} else {
    status = metrics_admission.Skipped
}
if resource == metrics_admission.Pod {
    metrics_admission.OnAdmittedPod(status == metrics_admission.Applied)
}
```

### 完整 VPA 工作链路

```
Pod 创建请求 (kubectl apply / Deployment 重建)
  │
  ▼ kube-apiserver → MutatingWebhookConfiguration
  │                   → admission-controller HTTPS server
  │
  ▼ Serve() → admit() → GetPatches()
  │   ① GetMatchingVPA          找到控制该 Pod 的 VPA
  │   ② CalculatePatches        读取 VPA.status.recommendation.target
  │   ③ 生成 JSON Patch         修改 container.resources.requests
  │
  ▼ AdmissionResponse (JSONPatch)
  │   patch: [{"op":"add","path":"/spec/containers/0/resources/requests/cpu","value":"1038m"}]
  │
  ▼ Pod 以新的 resource request 启动
     container.resources.requests.cpu = 1038m (VPA 推荐值)
     container.resources.requests.memory = 262144k
```
