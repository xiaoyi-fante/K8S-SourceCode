# 第34章 k8s HPA 扩容和 Aggregator 汇聚插件原理解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 34 章 — k8s HPA 扩容和 Aggregator 汇聚插件原理解读
> **源码入口**: `cmd/kube-controller-manager/app/autoscaling.go → startHPAController:37`

---

## 核心机制一览

1. **HPA 的本质是两件事**：监控 Pod 的负载（通过 metrics client 从 metrics-server / prometheus-adapter 获取指标），以及根据指标与阈值的比值控制 Deployment 的副本数（通过 scaleNamespacer.Scale 写回 apiserver）。

2. **三类 metrics client 对应三个 API group**：资源型（CPU/内存）→ `metrics.k8s.io`（由 metrics-server 提供）；自定义型 → `custom.metrics.k8s.io`（由 prometheus-adapter 提供）；外部型 → `external.metrics.k8s.io`。三者统一封装在 `NewRESTMetricsClient` 中，HPA 控制器通过这一个 client 访问所有类型的指标。

3. **desiredReplicas 计算核心公式**：`desiredReplicas = ceil(currentReplicas × (currentUtilization / targetUtilization))`。对 missing pods（metrics 数据缺失）的处理有方向性——缩容时按 100% 利用率处理（保守缩），扩容时按 0% 利用率处理（保守扩），避免误判。

4. **稳定窗口防抖动**：`downscaleStabilisationWindow`（默认 5 分钟）保留历史 recommendation，取最大值作为最终 desiredReplicas，防止负载短时下降导致反复扩缩。扩容无此限制，立即生效。

5. **Aggregator 是 apiserver 的代理扩展层**：`kube-aggregator` 作为 apiserver chain 的第一层，监听 APIService 对象。当请求路径匹配某 APIService 的 group/version 时，Aggregator 把请求反向代理（`ReverseProxy`）转发给对应的扩展 apiserver（如 metrics-server）；否则透传给内层 kube-apiserver。这样 metrics.k8s.io 的 API 和 k8s 核心 API 都通过同一个地址访问。

---

## 全章调用链总图

```
startHPAController（autoscaling.go:37）
  │
  ▼ startHPAControllerWithRESTClient（:50）
  │  apiVersionsGetter → PeriodicInvalidate preferred version cache
  │  metricsClient = metrics.NewRESTMetricsClient(
  │      resourceclient.NewForConfigOrDie,       → metrics.k8s.io
  │      custom_metrics.NewForConfig,            → custom.metrics.k8s.io
  │      external_metrics.NewForConfigOrDie,     → external.metrics.k8s.io
  │  )
  │
  ▼ startHPAControllerWithMetricsClient（:82）
  │  scaleClient = scale.NewForConfig(...)
  │  replicaCalc = NewReplicaCalculator(metricsClient, podLister, ...)
  │  go NewHorizontalController(...).Run(stopCh)
  │
  ▼ HorizontalController.Run（horizontal.go:165）
  │  cache.WaitForNamedCacheSync
  │  go wait.Until(a.worker, time.Second, stopCh)
  │
  ▼ worker（:212）→ processNextWorkItem → reconcileKey（:353）
  │  hpa := hpaLister.Get(name)
  │  reconcileAutoscaler(hpa, key)
  │
  ▼ reconcileAutoscaler（:574）
  │  scale := scaleNamespacer.Get(targetRef)
  │  currentReplicas := scale.Spec.Replicas
  │  computeReplicasForMetrics（:248）
  │    │  遍历 hpa.Spec.Metrics
  │    │  → computeStatusForResourceMetric
  │    │      → GetRawResourceReplicas（replica_calculator.go:153）
  │    │          → calcPlainMetricReplicas（:177）
  │    │              → groupPods（:377） 分类 ready/unready/missing/ignored
  │    │              → GetMetricUtilizationRatio（utilization.go:57）
  │    │                  → 计算 usageRatio = metricsTotal / targetTotal
  │    │              → 处理 missingPods / unreadyPods
  │    │              → 计算 replicaCount = ceil(currentReplicas × usageRatio)
  │    │  → 取所有 metric 中 desiredReplicas 最大值
  │  调整 desiredReplicas（上下限 / 稳定窗口）
  │  scaleNamespacer.Scale(context, targetGVR, ..., desiredReplicas)

Aggregator 启动链
  │
  ▼ createAggregatorServer（kube-apiserver/app/server.go）
  │  aggregatorConfig.NewWithDelegate(delegationTarget)（apiserver.go:169）
  │    → 注册 apiregistration.k8s.io group 的路由
  │    → 启动 APIServiceRegistrationController
  │
  ▼ APIServiceRegistrationController.Run
  │  → sync(key) → apiHandlerManager.AddAPIService(apiService)（:368）
  │      → proxyHandler = &proxyHandler{delegateHandler, ...}
  │        路径：/apis/{group}/{version}
  │        注册到 GenericAPIServer.Handler.NonGoRestfulMux
  │      → apiGroupHandler（返回 groupVersion 发现信息）
  │        路径：/apis/{group}
  │
  ▼ 请求到达 /apis/metrics.k8s.io/v1beta1/...
      proxyHandler.ServeHTTP
        → UpgradeAwareHandler.ServeHTTP
          → ReverseProxy.ServeHTTP（反向代理到 metrics-server service）
```

---

## §01 基于 CPU 的 HPA 扩容

### HPA 简介

HPA（Horizontal Pod Autoscaler）通过对 Pod 负载的监控，自动增加或减少 Pod 的副本数。

`autoscaling/v1` 只支持基于 CPU 的 HPA；`autoscaling/v2beta2` 支持内存和自定义指标。

### 部署基于 CPU 的 HPA 实验

**第一步：部署 metrics-server**

```bash
kubectl apply -f metrics-server.yaml
kubectl get pod -n kube-system | grep metrics-server
```

metrics-server 从 kubelet 采集各节点和 Pod 的 CPU/内存使用情况，并通过 `metrics.k8s.io` API 暴露出来。

**第二步：创建测试 Deployment**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hpa-demo01
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx-hpa-demo01
  template:
    metadata:
      labels:
        app: nginx-hpa-demo01
    spec:
      containers:
        - name: nginx
          image: nginx
          ports:
            - containerPort: 80
          resources:
            requests:
              cpu: 50m          # HPA 计算利用率需要 requests 字段
```

**第三步：创建 HPA 对象**

```yaml
apiVersion: autoscaling/v1
kind: HorizontalPodAutoscaler
metadata:
  name: nginx-hpa01
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: hpa-demo01
  minReplicas: 1
  maxReplicas: 5
  targetCPUUtilizationPercentage: 50    # CPU 利用率目标 50%
```

**第四步：施加压力触发扩容**

```bash
# 在压测 pod 中持续向 nginx 发请求
kubectl run -it --rm load-gen --image=busybox /bin/sh
while true; do wget -q -O- http://<svc-ip>; done

# 观察 HPA 状态
kubectl get hpa nginx-hpa01 --watch
# NAME        REFERENCE          TARGETS   MINPODS  MAXPODS  REPLICAS
# nginx-hpa01 Deployment/hpa-demo01  135%/50%  1       5        3
```

当 CPU 利用率超过 50% 时，HPA 自动扩副本；停止压测后约 5 分钟（downscaleStabilisationWindow）自动缩回。

### targetAverageUtilization vs targetAverageValue

| 字段 | 含义 | 计算方式 |
|------|------|---------|
| `targetAverageUtilization` | 百分比，相对于 requests | `currentUsage / podRequests × 100`，对比目标百分比 |
| `targetAverageValue` | 绝对值 | 直接以当前使用量对比目标值 |

---

## §02 基于内存的 HPA 扩容

### 内存压测原理

**tmpfs** 是一个临时文件系统，驻留在内存中，`/dev/shm` 目录不在硬盘上，而在内存里。由于在内存里，读写非常快；Linux 下 tmpfs 默认最大为内存的一半。

**dd 命令**：以指定大小的块拷贝文件，并在拷贝同时进行转换。

```bash
# 向 tmpfs 写入 40MB 数据，模拟内存使用增加
dd if=/dev/zero of=/tmp/memory/block bs=1M count=40
# 删除文件释放内存
rm -f /tmp/memory/block
```

### 部署基于内存的 HPA

内存 HPA 需要使用 `autoscaling/v2beta1` API（稳定版 v1 不支持内存）：

```yaml
apiVersion: autoscaling/v2beta1
kind: HorizontalPodAutoscaler
metadata:
  name: nginx-hpa-mem01
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: hpa-mem-demo01
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: memory
        targetAverageUtilization: 60   # 内存利用率目标 60%
```

测试 Deployment 需要挂载脚本（通过 ConfigMap），并设置 `securityContext.privileged: true`（tmpfs 写入需要权限）：

```yaml
containers:
  - name: nginx
    image: nginx
    resources:
      requests:
        memory: 50Mi
        cpu: 50m
    securityContext:
      privileged: true
    volumeMounts:
      - name: increase-mem-script
        mountPath: /etc/script
volumes:
  - name: increase-mem-script
    configMap:
      name: increase-mem-config
```

脚本 `increase-mem.sh`：
```bash
#!/bin/bash
mkdir -p /tmp/memory
mount -t tmpfs -o size=40m tmpfs /tmp/memory
dd if=/dev/zero of=/tmp/memory/block bs=1M count=40
sleep 60
rm /tmp/memory/block
umount /tmp/memory
```

执行脚本后，内存使用率超过 60% → HPA 扩容；sleep 60 秒后脚本删除文件释放内存 → 使用率下降 → 约 5 分钟后 HPA 缩回 1 个副本。

---

## §03 HPA 控制器源码解读之三种监控指标 client

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| HPA 控制器注册 | [autoscaling.go](kubernetes/cmd/kube-controller-manager/app/autoscaling.go) | `startHPAController:37` |
| metrics client 构造 | [autoscaling.go](kubernetes/cmd/kube-controller-manager/app/autoscaling.go) | `startHPAControllerWithRESTClient:50` |
| HPA 控制器构造 | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `NewHorizontalController:106` |

### 三种 metrics 来源

监控 Pod 负载的三种 metrics 来源：

| 类型 | API group | 数据来源 |
|------|-----------|---------|
| 资源型（CPU/内存） | `metrics.k8s.io` | metrics-server |
| 自定义型 | `custom.metrics.k8s.io` | prometheus-adapter |
| 外部型 | `external.metrics.k8s.io` | 外部系统（如云厂商监控） |

### startHPAController 启动链

```go
// cmd/kube-controller-manager/app/autoscaling.go:37
func startHPAController(ctx ControllerContext) (http.Handler, bool, error) {
    if !ctx.AvailableResources[schema.GroupVersionResource{Group: "autoscaling", Version: "v1", ...}] {
        return nil, false, nil
    }
    return startHPAControllerWithRESTClient(ctx)
}
```

`startHPAControllerWithRESTClient`（:50）：

```go
func startHPAControllerWithRESTClient(ctx ControllerContext) (http.Handler, bool, error) {
    clientConfig := ctx.ClientBuilder.ConfigOrDie("horizontal-pod-autoscaler")
    hpaClient := ctx.ClientBuilder.ClientOrDie("horizontal-pod-autoscaler")

    // apiVersionsGetter 定期刷新 preferred version 缓存
    apiVersionsGetter := custom_metrics.NewAvailableAPIsGetter(hpaClient.Discovery())
    go custom_metrics.PeriodicallyInvalidate(
        apiVersionsGetter,
        ctx.ComponentConfig.HPAController.HorizontalPodAutoscalerSyncPeriod.Duration,
        ctx.Stop,
    )

    // 创建统一的 metrics client，封装三种类型
    metricsClient := metrics.NewRESTMetricsClient(
        resourceclient.NewForConfigOrDie(clientConfig),              // metrics.k8s.io
        custom_metrics.NewForConfig(clientConfig, ctx.RESTMapper, apiVersionsGetter), // custom.metrics.k8s.io
        external_metrics.NewForConfigOrDie(clientConfig),            // external.metrics.k8s.io
    )
    return startHPAControllerWithMetricsClient(ctx, metricsClient)
}
```

### 三个 client 的 API group 对应关系

**01 resourceclient（metrics.k8s.io）**

```go
// staging/src/k8s.io/metrics/pkg/apis/metrics/v1beta1/...
const GroupName = "metrics.k8s.io"
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}
```

通过 `kubectl get apiservice` 可以看到 `v1beta1.metrics.k8s.io` 对应 `kube-system/metrics-server`——这个 APIService 是部署 metrics-server 时创建的：

```yaml
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1beta1.metrics.k8s.io
spec:
  group: metrics.k8s.io
  version: v1beta1
  service:
    name: metrics-server
    namespace: kube-system
```

访问 `kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes` 实际由 Aggregator 代理转发到 metrics-server。

**02 custom_metrics（custom.metrics.k8s.io）**

```go
const GroupName = "custom.metrics.k8s.io"
```

`NewForConfig` 返回一个 `multiClient`，通过 `getPreferredClient()` → `PreferredVersion()` 获取 apiVersionsGetter 中缓存的最新版本，再 `newClient(pref)` 创建真正的 client。`fetchVersions` 调用 `Discovery().ServerGroups()` 从 apiserver 发现已注册的 `custom.metrics.k8s.io` 的版本列表（由 prometheus-adapter 注册）。

**03 external_metrics（external.metrics.k8s.io）**

```go
const GroupName = "external.metrics.k8s.io"
```

构造方式与 resourceclient 类似，直接通过 `NewForConfigOrDie` 创建固定版本的 client。

### NewHorizontalController：控制器构造

```go
// pkg/controller/podautoscaler/horizontal.go:106
func NewHorizontalController(...) *HorizontalController {
    broadcaster := record.NewBroadcaster()
    broadcaster.StartRecordingToSink(...)
    recorder := broadcaster.NewRecorder(...)

    hpaController := &HorizontalController{
        eventRecorder:              recorder,
        scaleNamespacer:            scaleNamespacer,
        hpaNamespacer:              hpaNamespacer,
        downscaleStabilisationWindow: downscaleStabilisationWindow,
        queue:          workqueue.NewNamedRateLimitingQueue(...),
        mapper:         mapper,
        recommendations:    map[string][]timestampedRecommendation{},
        scaleUpEvents:   map[string][]timestampedScaleEvent{},
        scaleDownEvents: map[string][]timestampedScaleEvent{},
    }

    // HPA Informer 事件 → enqueueHPA
    hpaInformer.Informer().AddEventHandlerWithResyncPeriod(
        cache.ResourceEventHandlerFuncs{
            AddFunc:    hpaController.enqueueHPA,
            UpdateFunc: hpaController.updateHPA,
            DeleteFunc: hpaController.deleteHPA,
        },
        resyncPeriod,
    )

    // podLister 用于 groupPods 阶段的 pod 信息查询
    hpaController.podLister = podInformer.Lister()

    // replicaCalc 封装所有副本数计算逻辑
    replicaCalc := NewReplicaCalculator(
        metricsClient,
        hpaController.podLister,
        tolerance,
        cpuInitializationPeriod,
        delayOfInitialReadinessStatus,
    )
    hpaController.replicaCalc = replicaCalc
    return hpaController
}
```

`replicaCalc` 是副本计算的核心对象，封装了从 metrics client 取数据、过滤 Pod 状态、计算目标副本数的所有逻辑。

---

## §04 HPA 控制器源码解读之调谐过程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 主循环 | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `Run:165` |
| worker | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `worker:212` |
| reconcileKey | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `reconcileKey:353` |
| reconcileAutoscaler | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `reconcileAutoscaler:574` |
| 副本数计算入口 | [horizontal.go](kubernetes/pkg/controller/podautoscaler/horizontal.go) | `computeReplicasForMetrics:248` |
| 原始指标副本数 | [replica_calculator.go](kubernetes/pkg/controller/podautoscaler/replica_calculator.go) | `GetRawResourceReplicas:153` |
| 纯指标副本数计算 | [replica_calculator.go](kubernetes/pkg/controller/podautoscaler/replica_calculator.go) | `calcPlainMetricReplicas:177` |
| Pod 分组 | [replica_calculator.go](kubernetes/pkg/controller/podautoscaler/replica_calculator.go) | `groupPods:377` |
| 利用率比计算 | [utilization.go](kubernetes/pkg/controller/podautoscaler/metrics/utilization.go) | `GetResourceUtilizationRatio:26` |
| 指标利用率比 | [utilization.go](kubernetes/pkg/controller/podautoscaler/metrics/utilization.go) | `GetMetricUtilizationRatio:57` |

### Run → worker → reconcileKey

```go
// horizontal.go:165
func (a *HorizontalController) Run(stopCh <-chan struct{}) {
    // 等待 hpaLister 和 podLister 同步
    if !cache.WaitForNamedCacheSync("HPA", stopCh, a.hpaListerSynced, a.podListerSynced) {
        return
    }
    go wait.Until(a.worker, time.Second, stopCh)
    <-stopCh
}

// worker：消费队列，调用 processNextWorkItem
func (a *HorizontalController) worker() {
    for a.processNextWorkItem() { }
}

// processNextWorkItem → reconcileKey
func (a *HorizontalController) reconcileKey(key string) (deleted bool, err error) {
    namespace, name, _ := cache.SplitMetaNamespaceKey(key)
    hpa, err := a.hpaLister.HorizontalPodAutoscalers(namespace).Get(name)
    if errors.IsNotFound(err) {
        // HPA 被删除，清理历史记录
        delete(a.recommendations, key)
        delete(a.scaleUpEvents, key)
        delete(a.scaleDownEvents, key)
        return true, nil
    }
    return false, a.reconcileAutoscaler(hpa, key)
}
```

### reconcileAutoscaler：调谐核心

```go
// horizontal.go:574（简化）
func (a *HorizontalController) reconcileAutoscaler(hpav1Shared *autoscalingv1.HorizontalPodAutoscaler, key string) error {
    hpa := hpav1Shared.(*autoscalingv2.HorizontalPodAutoscaler)

    // 获取被管理的资源（Deployment 等）的当前 scale 对象
    scale, targetGVR, err := a.scaleForResourceMappings(hpa.Namespace, hpa.Spec.ScaleTargetRef.Name, mappings)
    currentReplicas := scale.Spec.Replicas

    // 副本数处于上下限之间才需要调谐
    if currentReplicas > hpa.Spec.MaxReplicas {
        desiredReplicas = hpa.Spec.MaxReplicas
    } else if currentReplicas < *hpa.Spec.MinReplicas {
        desiredReplicas = *hpa.Spec.MinReplicas
    } else {
        // 根据 metrics 计算目标副本数
        metricDesiredReplicas, _, _, _, err = a.computeReplicasForMetrics(hpa, scale, hpa.Spec.Metrics)
        // 用 metricDesiredReplicas 调整 desiredReplicas（取更大值防抖）
    }

    // 确定 rescaleReason（扩容/缩容/保持）
    if desiredReplicas > currentReplicas {
        rescaleReason = fmt.Sprintf("All metrics above target")
    } else if desiredReplicas < currentReplicas {
        // 缩容受 downscaleStabilisationWindow 约束
    }

    // 最终执行 scale
    if rescale {
        scale.Spec.Replicas = desiredReplicas
        _, err = a.scaleNamespacer.Scales(hpa.Namespace).Update(context.TODO(), targetGVR, scale, ...)
    }
}
```

**downscaleStabilisationWindow 防抖动**：

- 每次调谐结果（desiredReplicas）都记录到 `recommendations` map 中（带时间戳）
- 缩容时，从 window 内的所有历史记录中取**最大值**作为最终 desiredReplicas
- 扩容无此限制，立即取最新计算结果

### computeReplicasForMetrics：按指标类型分发

```go
// horizontal.go:248
func (a *HorizontalController) computeReplicasForMetrics(...) {
    for i, metricSpec := range metricSpecs {
        replicaCountProposal, metricNameProposal, _, err := a.computeReplicasForMetric(hpa, metricSpec, ...)
        // 取所有 metric 中 replicaCountProposal 最大值
    }
}
```

按 `metricSpec.Type` 分四类处理：
- `Resource`：调用 `computeStatusForResourceMetric` → `GetRawResourceReplicas`
- `Pods`：使用 `metricsClient.GetRawMetric` 获取自定义 pod metrics
- `Object`：使用 `metricsClient.GetObjectMetric`
- `External`：使用 `metricsClient.GetExternalMetric`

### GetRawResourceReplicas → calcPlainMetricReplicas

```go
// replica_calculator.go:153
func (c *ReplicaCalculator) GetRawResourceReplicas(currentReplicas int32, targetUtilization int64, resource v1.ResourceName, ...) {
    // 从 metrics-server 获取所有 pod 的 metrics
    metrics, timestamp, err := c.metricsClient.GetRawMetric(resource, namespace, selector, container)
    // 调用 calcPlainMetricReplicas 计算目标副本数
    replicaCount, utilization, err = c.calcPlainMetricReplicas(metrics, currentReplicas, targetUtilization, ...)
}
```

### calcPlainMetricReplicas：副本数计算

```go
// replica_calculator.go:177
func (c *ReplicaCalculator) calcPlainMetricReplicas(metrics PodMetricsInfo, currentReplicas int32, targetUtilization int64, ...) {
    // 1. 从 pod-informer 获取 pod 列表
    podList, err := c.podLister.Pods(namespace).List(selector)

    // 2. 分组 Pod
    readyPodCount, unreadyPods, missingPods, ignoredPods = groupPods(podList, metrics, resource, ...)

    // 3. 从 metrics map 剔除 ignoredPods 和 unreadyPods
    removeMetricsForPods(metrics, ignoredPods)
    removeMetricsForPods(metrics, unreadyPods)

    // 4. 计算平均利用率（仅基于 readyPods）
    usageRatio, utilization = GetMetricUtilizationRatio(metrics, targetUtilization)

    // 5. 处理 missingPods（保守策略）
    if len(missingPods) > 0 {
        if usageRatio < 1.0 {
            // 缩容方向：missing pods 按 100% 利用率处理（保守缩）
            metrics[podName] = PodMetric{Value: targetUtilization}
        } else {
            // 扩容方向：missing pods 按 0% 利用率处理（保守扩）
            metrics[podName] = PodMetric{Value: 0}
        }
    }

    // 6. 扩容时 unreadyPods 利用率设为 0
    if !rebalanceIgnored {
        for podName := range unreadyPods {
            metrics[podName] = PodMetric{Value: 0}
        }
    }

    // 7. 重新计算利用率（含 missing/unready pods 的修正值）
    newUsageRatio, _ = GetMetricUtilizationRatio(metrics, targetUtilization)

    // 8. 变化量太小则不 scale
    if math.Abs(1.0-newUsageRatio) <= c.tolerance {
        return currentReplicas, utilization, nil
    }

    // 9. 计算目标副本数
    replicaCount = int32(math.Ceil(float64(readyPodCount) * float64(newUsageRatio)))
}
```

### groupPods：Pod 状态分类

```go
// replica_calculator.go:377
func groupPods(pods []*v1.Pod, metrics PodMetricsInfo, resource v1.ResourceName, ...) {
    missingPods  = sets.NewString()   // metrics 数据缺失
    unreadyPods  = sets.NewString()   // Pending 状态
    ignoredPods  = sets.NewString()   // 正在删除 / PodFailed

    for _, pod := range pods {
        if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodFailed {
            ignoredPods.Insert(pod.Name)
            continue
        }
        if pod.Status.Phase == v1.PodPending {
            unreadyPods.Insert(pod.Name)
            continue
        }
    }
    // missingPods = 在 pod list 中但不在 metrics map 中的 pod
}
```

### GetResourceUtilizationRatio：百分比利用率计算

```go
// utilization.go:26
func GetResourceUtilizationRatio(metrics PodMetricsInfo, requests map[string]int64, targetUtilization int32) (utilizationRatio float64, currentUtilization int32, ...) {
    metricsTotal := int64(0)
    requestsTotal := int64(0)
    for podName, metric := range metrics {
        request, hasRequest := requests[podName]
        if !hasRequest { continue }
        metricsTotal  += metric.Value
        requestsTotal += request
        numEntries++
    }
    // 当前利用率 = (metricsTotal × 100) / requestsTotal
    currentUtilization = int32((metricsTotal * 100) / requestsTotal)
    // usageRatio = currentUtilization / targetUtilization
    return float64(currentUtilization) / float64(targetUtilization), currentUtilization, ...
}
```

`usageRatio > 1` 说明需要扩容，`usageRatio < 1` 说明可以缩容；最终 `desiredReplicas = ceil(readyPodCount × usageRatio)`。

---

## §05 k8s apiserver 的 Aggregator 汇聚插件

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Aggregator 创建 | [apiserver.go](kubernetes/staging/src/k8s.io/kube-aggregator/pkg/apiserver/apiserver.go) | `NewWithDelegate:169` |
| APIService 注册 | [apiserver.go](kubernetes/staging/src/k8s.io/kube-aggregator/pkg/apiserver/apiserver.go) | `AddAPIService:368` |

### Aggregator 是什么

kube-aggregator 是 apiserver 的一个扩展机制，允许开发者向 k8s 集群添加**自己的 API**，就像添加了新的资源类型一样。

- **核心 API**：由 kube-apiserver 提供，如 Pod、Deployment 等
- **扩展 API**：开发者自己实现的 API Server，通过 Aggregator 代理后对外统一提供

Aggregator 作为 apiserver delegate chain 的**第一层**（最前面），负责：
1. 把 `/apis/{group}/{version}` 请求路由到对应的扩展 apiserver（反向代理）
2. 其余请求透传给内层 kube-apiserver
3. 提供 `/apis/{group}` 发现端点（返回该 group 下所有版本）

### Aggregator 启动：NewWithDelegate

```go
// staging/src/k8s.io/kube-aggregator/pkg/apiserver/apiserver.go:169
func (c completedConfig) NewWithDelegate(delegationTarget genericapiserver.DelegationTarget) (*APIAggregator, error) {
    // ...
    // 注册 apiregistration.k8s.io 的路由（APIService 对象的 CRUD）
    apiGroupInfo := apiserver.NewDefaultAPIGroupInfo(Registration.GroupName, ...)
    s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo)

    // 启动 APIServiceRegistrationController
    apiServiceRegistrationController := NewAPIServiceRegistrationController(informerFactory.Apiregistration().V1().APIServices(), s)
    // ...
}
```

### APIServiceRegistrationController.Run：监听 APIService 变化

```go
// 控制器监听 APIService informer，有变化时调用 sync
func (c *APIServiceRegistrationController) sync(key string) error {
    apiService, err := c.apiServiceLister.Get(key)
    if apierrors.IsNotFound(err) {
        c.apiHandlerManager.RemoveAPIService(key)
        return nil
    }
    return c.apiHandlerManager.AddAPIService(apiService)
}
```

### AddAPIService：注册代理 Handler

```go
// apiserver.go:368
func (s *APIAggregator) AddAPIService(apiService *v1.APIService) error {
    // 构建 proxyHandler（反向代理）
    proxyPath := "/apis/" + apiService.Spec.Group + "/" + apiService.Spec.Version

    proxyHandler := &proxyHandler{
        localDelegate:            s.delegateHandler,    // 本地 kube-apiserver
        proxyCurrentCertKeyContent: s.proxyCurrentCertKeyContent,
        proxyTransport:           s.proxyTransport,
        serviceResolver:          s.serviceResolver,    // 解析 service 地址
        egressSelector:           s.egressSelector,
    }
    s.proxyHandlers[apiService.Name] = proxyHandler

    // 注册到 NonGoRestfulMux（路径：/apis/group/version）
    s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(proxyPath, proxyHandler)
    s.GenericAPIServer.Handler.NonGoRestfulMux.UnlistedHandlePrefix(proxyPath+"/", proxyHandler)

    // 注册 apiGroupHandler（路径：/apis/group）
    groupPath := "/apis/" + apiService.Spec.Group
    groupDiscoveryHandler := &apiGroupHandler{
        codecs:    aggregatorscheme.Codecs,
        groupName: apiService.Spec.Group,
        lister:    s.lister,
        delegate:  s.delegateHandler,
    }
    s.GenericAPIServer.Handler.NonGoRestfulMux.Handle(groupPath, groupDiscoveryHandler)
    s.handledGroups.Insert(apiService.Spec.Group)
    return nil
}
```

### 请求转发流程

```
用户请求：GET /apis/metrics.k8s.io/v1beta1/namespaces/kube-system/pods/...
  │
  ▼ Aggregator（第一层）
  │  NonGoRestfulMux 匹配 /apis/metrics.k8s.io/v1beta1/*
  │  → proxyHandler.ServeHTTP
  │
  ▼ proxyHandler.ServeHTTP
  │  → proxy.NewUpgradeAwareHandler(location, proxyRoundTripper, ...)
  │  → UpgradeAwareHandler.ServeHTTP
  │
  ▼ ReverseProxy.ServeHTTP
     serviceResolver.ResolveEndpoint → kube-system/metrics-server:443
     反向代理请求到 metrics-server pod
     metrics-server 返回 PodMetrics 数据
```

**proxyPath** 构成：`/apis/` + group + `/` + version，例如 `/apis/metrics.k8s.io/v1beta1`。

**apiGroupHandler** 处理 `/apis/metrics.k8s.io` 路径，从 lister 查找当前所有注册的 APIService，返回 APIGroup 发现信息（versions 列表）：

```json
{
  "kind": "APIGroup",
  "apiVersion": "v1",
  "name": "metrics.k8s.io",
  "versions": [{ "groupVersion": "metrics.k8s.io/v1beta1", "version": "v1beta1" }],
  "preferredVersion": { "groupVersion": "metrics.k8s.io/v1beta1", "version": "v1beta1" }
}
```

### 从 metrics-server 获取指标的鉴权

直接 curl 访问 `https://10.96.0.1/apis/metrics.k8s.io/v1beta1/...` 会报 403。需要带 ServiceAccount token，并且该 SA 需要 ClusterRole 中包含 `metrics.k8s.io` apiGroup 的权限：

```yaml
- apiGroups:
    - metrics.k8s.io
  resources:
    - '*'
  verbs: ["get", "list", "watch"]
```

### 完整 HPA 架构图

```
kube-controller-manager                    kube-apiserver 侧
  │                                              │
  │  HPA 控制器                       Aggregator 汇聚插件
  │   ├─ metric-client                     │
  │   │   ├─ metrics.k8s.io      ←────────┤ 代理转发
  │   │   ├─ custom.metrics.k8s.io         │
  │   │   └─ external.metrics.k8s.io       │      metrics-server
  │   │                                    │           │
  │   └─ hpa-scale-informer               │      kubelet cAdvisor
  │       计算副本数并 scale              │           │
  │                                        │      各 Pod 指标
  │                                        │
  └── scaleNamespacer.Scale ──────────────→ Deployment replicas 更新
                                           scheduler 调度新 pod
```
