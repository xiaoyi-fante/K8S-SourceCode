# 第37章 metrics-server 源码解读与 kubectl top 原理

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 37 章 — k8s hpa 和 vpa 依赖的 metrics-server 源码解读和 kubelet top 原理
> **源码入口**: `staging/src/k8s.io/kubectl/pkg/cmd/top/top.go`（kubectl top）；metrics-server 为外部项目 `kubernetes-sigs/metrics-server`

---

## 核心机制一览

1. **metrics-server 是 HPA/VPA 的数据源**：HPA 查 CPU/内存指标、VPA Recommender 查历史资源用量，都经过 `metrics.k8s.io` API group。metrics-server 通过 kube-aggregator 聚合插件注册为该 group 的实现，apiserver 把请求代理到 metrics-server。

2. **数据从 kubelet cadvisor 拉取**：metrics-server 直接 GET kubelet 的 `/metrics/resource` HTTP 接口（Prometheus exposition 格式），不依赖 Prometheus。拉取到的是节点上所有 Pod/容器的 CPU 和内存瞬时值。

3. **CPU 需要两个数据点才能算 rate**：CPU 是累积计数器（单位 nanoseconds），必须用 `(last - prev) / ts_delta` 才能得到使用率；内存直接用 last 的值。因此 storage 为每个 node/pod 保存 last 和 prev 两个时间点的数据。

4. **metrics-server 作为 Extension APIServer 注册**：通过 `server.InstallAPIGroup` 向 genericAPIServer 注册 `metrics.k8s.io/v1beta1` group，提供 `nodes` 和 `pods` 两种资源，分别对应 `NodeMetrics` 和 `PodMetrics` 类型。外部使用 `kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes` 即可验证。

5. **kubectl top node/pod 的分子/分母分别来自不同 API**：分子（实际用量）从 `metrics.k8s.io` 拿，分母（可分配量）从 `NodeStatus.Allocatable` 拿，`PrintNodeMetrics` 负责计算百分比并格式化输出。

---

## 全章调用链总图

```
kubectl top node / pod
  │
  ▼ NewCmdTop (top.go:47)
  │   ├── NewCmdTopNode (top_node.go:70)
  │   └── NewCmdTopPod  (top_pod.go:86)
  │
  ▼ RunTopNode / RunTopPod
  │   ├── SupportedMetricsAPIVersionAvailable — 检查 metrics.k8s.io 是否就绪
  │   ├── getNodeMetricsFromMetricsAPI        — GET /apis/metrics.k8s.io/v1beta1/nodes
  │   ├── NodeClient.Nodes().List/Get         — 取 NodeStatus.Allocatable (分母)
  │   └── Printer.PrintNodeMetrics            — 计算百分比并打印
  │
  ▼ metrics.k8s.io apiserver (metrics-server)
  │
  ▼ podMetrics / nodeMetrics REST handler
  │   └── storage.GetPodMetrics / GetNodeMetrics
  │           └── podStorage.GetMetrics — 从内存 store 取 last/prev
  │
  ▼ runScrape (server.go) — ticker 每 10s 触发
  │   └── scraper.Scrape(ctx)  (scraper.go)
  │           ├── 并发 goroutine 每个 node 发 HTTP
  │           └── kubeletClient.GetMetrics → GET /metrics/resource
  │
  ▼ kubelet cadvisor /metrics/resource
      (Prometheus exposition 格式：container_cpu_usage_seconds_total 等)
```

---

## §01 metrics-server 源码解读

| 读码目标 | 源文件（外部项目，不在本地 k8s 源码） | 入口函数 |
|---------|--------------------------------------|---------|
| 程序入口 | `cmd/metrics-server/metrics-server.go` | `main` |
| 构造 server 对象 | `pkg/server/config.go` | `Config.Complete` |
| 运行主循环 | `pkg/server/server.go` | `RunUntil` |
| 定时采集 | `pkg/server/server.go` | `runScrape` / `tick` |
| 拉取 kubelet 数据 | `pkg/scraper/scraper.go` | `Scraper.Scrape` |
| kubelet HTTP 调用 | `pkg/scraper/scraper.go` | `kubeletClient.GetMetrics` |
| 解析 Prometheus 数据 | `pkg/scraper/scraper.go` | `decodeBatch` |
| 内存存储 | `pkg/storage/storage.go` | `storage.Store` |
| node 存储 | `pkg/storage/node.go` | `nodeStorage.Store` |
| 对外暴露指标 | `pkg/storage/storage.go` | `GetNodeMetrics` / `GetPodMetrics` |

> **说明**：metrics-server 是独立项目 `github.com/kubernetes-sigs/metrics-server`，源码不在本地 kubernetes 目录，以下内容基于截图分析。

### 1. 入口与启动链

```
main()
  │ GOMAXPROCS = runtime.NumCPU()
  ▼
NewMetricsServerCommand(stopCh)   // cobra cmd，RunE = runCommand
  │
  ▼ runCommand(opts, stopCh)
  │   o.Validate()
  │   config, _ = o.ServerConfig()
  │   s, _      = config.Complete()   // 构造 server 对象
  └── s.RunUntil(stopCh)              // 主循环
```

### 2. config.Complete — 构造 server 对象

`Config.Complete()` 是核心组装步骤，按顺序完成：

```
config.Complete()
  │
  ├── podInformerFactory.ForResource("pods") → podInformer   // watch pod 列表
  ├── KubeClient → informer.Core().V1().Nodes()              // node informer
  │
  ├── NewScraper(nodeLister, kubeletClient, scrapeTimeout)   // 构造采集器
  │
  ├── store := storage.NewStorage(c.MetricResolution)        // 初始化内存存储
  │       type storage struct {
  │           mu    sync.RWMutex
  │           pods  podStorage
  │           nodes nodeStorage
  │       }
  │
  ├── metricsHandler := s.metricsHandler()
  │       // 初始化 genericServer，注册 metrics.k8s.io API
  │       genericAPIServer.Handler.NonGoRestfulMux.Handle("/metrics", metricsHandler)
  │
  ├── InstallAPIGroup(&info)       // 注册 metrics.k8s.io/v1beta1 到 genericAPIServer
  │
  └── NewServer(nodes.Informer(), podInformer.Informer(), genericServer, store, scrape, c.MetricResolution)
          s.RegisterProbes(podInformerFactory)
```

### 3. storage 内存模型

```
storage struct {
    mu    sync.RWMutex
    pods  podStorage
    nodes nodeStorage
}

// 对外接口
GetNodeMetrics(nodes ...*corev1.Node)             → []metrics.NodeMetrics
GetPodMetrics(pods ...*metav1.PartialObjectMetadata) → []metrics.PodMetrics
```

**为什么保存两个点（last + prev）**：CPU 是累积计数器，必须用差值除以时间差才能得到使用率：

```
cpu_rate = (last.CPU - prev.CPU) / (last.Timestamp - prev.Timestamp)
```

内存直接取 last 的 value，无需 delta。

`podStorage` 内部结构：

```go
type PodMetricsPoint struct {
    StartTime  time.Time           // 容器启动时间
    Timestamp  time.Time           // 指标采集时间
    Containers map[string]MetricPoint
}

type MetricPoint struct {
    CumulativeCPUUsed uint64   // nanoseconds，累积值
    MemoryUsage       uint64   // bytes，瞬时值
}
```

### 4. RunUntil — 主循环

```go
// pkg/server/server.go
func (s *server) RunUntil(stopCh <-chan struct{}) error {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go s.nodes.Run(stopCh)        // 启动 node informer
    go s.pods.Run(stopCh)         // 启动 pod informer

    // 等待 informer 同步完成，确保本地缓存有数据
    cache.WaitForCacheSync(stopCh, s.nodes.HasSynced)
    cache.WaitForCacheSync(stopCh, s.pods.HasSynced)

    go s.runScrape(ctx)           // 启动定时采集
    return s.GenericAPIServer.PrepareRun().Run(stopCh)  // 启动 API server
}
```

### 5. runScrape — 定时采集

```go
func (s *server) runScrape(ctx context.Context) {
    ticker := time.NewTicker(s.resolution)  // 默认 10s (metric-resolution)
    defer ticker.Stop()
    s.tick(ctx, time.Now())                 // 立即执行一次

    for {
        select {
        case startTime := <-ticker.C:
            s.tick(ctx, startTime)
        case <-ctx.Done():
            return
        }
    }
}

func (s *server) tick(ctx context.Context, startTime time.Time) {
    // ...
    data := s.scraper.Scrape(ctx)    // 调用 Scraper 获取所有 node 的数据
    s.storage.Store(data)            // 存入内存
}
```

### 6. Scraper.Scrape — 并发拉取每个 node

```
Scraper.Scrape(ctx)
  │
  ├── nodeLister.List() → 获取所有 node
  ├── 计算 delayPerSource（错开请求，避免 thundering herd）
  │
  ├── for range nodes:
  │     goroutine → collectNode(node)
  │                   │
  │                   └── kubeletClient.GetMetrics(ctx, node)
  │                           GET node:port/metrics/resource
  │
  └── responseChannel → merge 各 node 结果 → MetricsBatch
```

**并发控制**：每个 node 启动一个 goroutine，通过 `responseChannel` 收集结果，最后合并。同时对每个 node 加入随机 sleep `delayPerSource`，防止所有 node 同时发起请求。

### 7. kubeletClient.GetMetrics — 访问 cadvisor 接口

```go
// GetMetrics 访问 node 的 /metrics/resource 端口
func (kc *kubeletClient) GetMetrics(ctx context.Context, node *corev1.Node) (*storage.MetricsBatch, error) {
    port := kc.defaultPort
    // 优先使用 node.Status.DaemonEndpoints.KubeletEndpoint.Port
    url := url.URL{
        Scheme: kc.scheme,
        Host:   net.JoinHostPort(addr, strconv.Itoa(port)),
        Path:   "/metrics/resource",
    }
    return kc.getMetrics(ctx, url.String(), node.Name)
}
```

`/metrics/resource` 返回 Prometheus exposition 格式，包含四类指标：

| 指标名 | 含义 |
|--------|------|
| `container_cpu_usage_seconds_total` | 容器 CPU 累积（秒） |
| `container_memory_working_set_bytes` | 容器内存工作集 |
| `node_cpu_usage_seconds_total` | 节点 CPU 累积 |
| `node_memory_working_set_bytes` | 节点内存工作集 |

### 8. decodeBatch — 解析 Prometheus 数据

```
decodeBatch(b []byte, defaultTime, nodeName)
  │
  ├── 初始化 MetricsBatch{Nodes: map, Pods: map}
  ├── textparse.New(b, "") → Prometheus 文本解析器
  │
  └── for parser.Next():
        timeseries, maybeTimestamp, value = parser.Series()
        switch:
          case timeseriesMatchesName(nodeCpuUsageMetricName):
            parseNodeCpuUsageMetrics → 写入 MetricsBatch.Nodes[nodeName]
          case timeseriesMatchesName(containerCpuUsageMetricName):
            parseContainerCpuMetrics → 按 namespace/podName/containerName 写入 Pods map
          ...  (memory 同理)
```

解析结果按 `namespace/podName` 为 key 组织，container 作为 pod 下的子 map。

### 9. storage.Store — 存入内存

```go
func (s *storage) Store(batch *MetricsBatch) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.nodes.Store(batch)   // 先存 node
    s.pods.Store(batch)    // 再存 pod
}
```

**nodeStorage.Store** 的关键逻辑：

```
构造 lastNodes（本次）和 prevNodes（上次）两个 map
for nodeName, newPoint := range batch.Nodes:
    lastNodes[nodeName] = newPoint

if lastNodes[node].Timestamp >= prevNodes[node].Timestamp:
    // 正常情况：新数据更新 last，old last 变 prev
    s.last  = lastNodes
    s.prev  = prevNodes
else:
    // 新数据时间戳更早（不合理），丢弃
    drop
```

**podStorage.Store** 同理，key 为 `namespace/podName`。

### 10. 对外注册为 Extension APIServer

```go
// config.Complete 中
BuildAndRegisterAggregation(info, metrics.GroupName, scheme, ...)
// 注册 metrics.k8s.io/v1beta1 group，资源为 nodes 和 pods

// 验证：
kubectl get --raw /apis/metrics.k8s.io/v1beta1 | python -m json.tool
// → resources: [{kind: NodeMetrics, name: nodes}, {kind: PodMetrics, name: pods}]
```

`podMetrics` 和 `nodeMetrics` 实现了 apiserver 要求的 REST 接口：

```go
type podMetrics struct {
    groupResource schema.GroupResource
    metrics       PodMetricsGetter
    podLister     cache.GenericLister
}

func (m *podMetrics) Kind() string { return "PodMetrics" }        // rest.KindProvider
func (m *podMetrics) List(...)     { ... }                         // rest.Lister
func (m *podMetrics) Get(...)      { ... }                         // rest.Getter
```

`GetPodMetrics` 内部从 storage 查找 lastPod 和 prevPod，用 `(last-prev)/ts_delta` 计算 CPU rate，内存直接取 last。

---

## §02 kubectl top 原理

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| top 命令注册 | [top.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/top/top.go) | `NewCmdTop:47` |
| top node 命令 | [top_node.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/top/top_node.go) | `NewCmdTopNode:70` |
| top node 执行 | [top_node.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/top/top_node.go) | `RunTopNode:144` |
| 获取 node 指标 | [top_node.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/top/top_node.go) | `getNodeMetricsFromMetricsAPI:200` |
| top pod 执行 | [top_pod.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/top/top_pod.go) | `RunTopPod:165` |

### 1. 命令注册

`kubectl top` 注册在 cmd.go 的 Cluster Management Commands 组里：

```go
// staging/src/k8s.io/kubectl/pkg/cmd/cmd.go
{
    Message: "Cluster Management Commands:",
    Commands: []*cobra.Command{
        top.NewCmdTop(f, ioStreams),
        // ... drain, cordon, taint ...
    },
}
```

`NewCmdTop` 只创建父命令（Run 对应打印 help），再挂两个子命令：

```go
func NewCmdTop(f cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
    cmd := &cobra.Command{Use: "top", Run: cmdutil.DefaultSubCommandRun(streams.ErrOut)}
    cmd.AddCommand(NewCmdTopNode(f, nil, streams))
    cmd.AddCommand(NewCmdTopPod(f, nil, streams))
    return cmd
}
```

### 2. kubectl top node 流程

```
RunTopNode() (top_node.go:144)
  │
  ├── 1. labels.Parse(o.Selector)              — 解析 label selector
  │
  ├── 2. DiscoveryClient.ServerGroups()        — 检查 metrics.k8s.io API 是否就绪
  │         SupportedMetricsAPIVersionAvailable(apiGroups)
  │         若无 → 报错 "Metrics API not available"
  │
  ├── 3. getNodeMetricsFromMetricsAPI(...)      — 获取分子（实际用量）
  │         metricsClient.MetricsV1beta1().NodeMetricses()
  │         有 resourceName → nm.Get(...)       // GET /nodes/{name}
  │         无 resourceName → nm.List(...)      // GET /nodes
  │         底层 → REST GET /apis/metrics.k8s.io/v1beta1/nodes
  │
  ├── 4. NodeClient.Nodes().Get/List()         — 获取分母（Allocatable）
  │         node.Status.Allocatable            // CPU + Memory 可分配量
  │
  └── 5. Printer.PrintNodeMetrics(metrics, allocatable, ...)
              fraction = quantity / available   // 百分比
              printValue + printAllResourceUsages
```

```go
// top_node.go:144 RunTopNode 核心片段
// // staging/src/k8s.io/kubectl/pkg/cmd/top/top_node.go
allocatable := make(map[string]v1.ResourceList)
for _, n := range nodes {
    allocatable[n.Name] = n.Status.Allocatable   // 分母来自 NodeStatus
}
return o.Printer.PrintNodeMetrics(metrics.Items, allocatable, o.NoHeaders, o.SortBy)
```

```go
// getNodeMetricsFromMetricsAPI:200 — 底层 REST 调用
// // staging/src/k8s.io/kubectl/pkg/cmd/top/top_node.go
mc := metricsClient.MetricsV1beta1()
nm := mc.NodeMetricses()
if resourceName != "" {
    m, _ = nm.Get(context.TODO(), resourceName, metav1.GetOptions{})
    // → GET /apis/metrics.k8s.io/v1beta1/nodes/{name}
} else {
    versionedMetrics, _ = nm.List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
    // → GET /apis/metrics.k8s.io/v1beta1/nodes
}
```

**百分比计算**：

```go
func printAllResourceUsages(out io.Writer, metrics *ResourceMetricsInfo) {
    for res := range metrics.Metrics {
        quantity  := metrics.Metrics[res]            // 分子：来自 metrics-server
        available := metrics.Available[res]          // 分母：来自 NodeStatus.Allocatable
        fraction  := float64(quantity.MilliValue()) / float64(available.MilliValue()) * 100
        fmt.Fprintf(out, "%v%%\t", int64(fraction))
    }
}
```

### 3. kubectl top pod 流程

与 top node 类似，差异在于：

- 分子从 `metrics.k8s.io/v1beta1/namespaces/{ns}/pods` 拿（PodMetrics）
- pod top **没有百分比**（无 Allocatable 分母概念），只输出绝对值
- `--containers` flag：`PrintContainers=true` 时展开每个容器，而非 Pod 汇总

```go
// top_pod.go:165 RunTopPod 核心片段
metrics, _ = getMetricsFromMetricsAPI(o.MetricsClient, o.Namespace, o.ResourceName, o.AllNamespaces, selector)
return o.Printer.PrintPodMetrics(metrics.Items, o.PrintContainers, o.AllNamespaces, o.NoHeaders, o.SortBy)
```

```go
// getMetricsFromMetricsAPI:210 — namespace 判断
ns := metav1.NamespaceAll      // --all-namespaces
if !allNamespaces { ns = namespace }
if resourceName != "" {
    m, _ = metricsClient.MetricsV1beta1().PodMetricses(ns).Get(...)
    // → GET /apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods/{name}
} else {
    versionedMetrics, _ = metricsClient.MetricsV1beta1().PodMetricses(ns).List(...)
    // → GET /apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods
}
```

### 4. 实际效果演示

```bash
# top node —— 显示 CPU/Memory 用量和百分比
kubectl top node
# NAME          CPU(cores)  CPU%  MEMORY(bytes)  MEMORY%
# k8s-master01  483m        4%    1856Mi         48%

# top node 排序
kubectl top node --sort-by=cpu
kubectl top node --sort-by=memory

# 过滤指定 node
kubectl top node k8s-node01 --show-labels

# top pod（只有值，无百分比）
kubectl top pods --namespace=kube-system

# top pod —— 展开容器
kubectl top pods onepod-multicontiner02 --containers=true
# POD                        NAME   CPU(cores)  MEMORY(bytes)
# onepod-multicontiner02     curl   0m          36Ki
# onepod-multicontiner02     nginx  0m          1360Ki
```

**验证底层 raw 数据**：

```bash
# node 指标
kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes | python -m json.tool
# → {name: "k8s-master01", timestamp: "...", usage: {cpu: "527664922n", memory: "7867996Ki"}}

# pod 指标（指定 namespace/name）
kubectl get --raw /apis/metrics.k8s.io/v1beta1/namespaces/kube-system/pods/etcd-k8s-master01
# → {containers: [{name: "etcd", usage: {cpu: "48959234n", memory: "359592Ki"}}]}
```
