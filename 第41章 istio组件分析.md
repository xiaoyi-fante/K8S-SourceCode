# 第41章 Istio 组件分析

> **适用版本**: Istio 1.11.4
> **对应章节**: 第 41 章 — Istio 组件分析
> **源码入口**: `pilot/cmd/pilot-agent/main.go`（pilot-agent）、`pilot/cmd/pilot-discovery/main.go`（istiod）

---

## 核心机制一览

1. **两个二进制，三个角色**：Istio 数据平面只有一个镜像 `docker.io/istio/proxyv2`，但 pod 内实际运行两个进程——`pilot-agent`（Go）负责生命周期管理，`envoy`（C++）负责实际转发。`istio-ingressgateway` 和 `istio-proxy` sidecar 都是这个组合，区别只在启动子命令：gateway 用 `proxy router`，sidecar 用 `proxy sidecar`。

2. **pilot-agent 是 Envoy 的保姆**：`pilot-agent` 启动时做三件事——启动 SDS gRPC server（管理证书）、启动 xDS gRPC server（作为 ADS 代理连接 istiod）、用 `exec.Command` 拉起 Envoy 进程并守护它。Envoy 崩溃后 pilot-agent 负责重启。

3. **xDS 双向流**：pilot-agent 内嵌一个 `XdsProxy`，它一端作为 gRPC server 接收 Envoy 的 ADS 订阅（下游），另一端作为 gRPC client 连接 istiod 的 ADS server（上游）。数据面 Envoy → pilot-agent → istiod 的 xDS 链路让 pilot-agent 可以做本地健康检查、缓存和 delta 处理。

4. **istiod 三大功能内聚**：istiod 是 Pilot + Citadel + Galley 合并后的单体控制平面，提供：服务发现（watch K8s Service/Endpoint）、配置分发（将 VirtualService/DestinationRule 等 CRD 翻译为 Envoy xDS）、证书管理（SDS，给 Envoy 下发 workload 证书）。

5. **CRD controller 驱动 xDS 推送**：istiod 对 Gateway、VirtualService 等 Istio CRD 注册 Informer，事件变化时将变更放入 `cl.queue`，worker 消费队列后调用 `XdsUpdater.SvcUpdate` 触发增量 xDS 推送给所有连接的 Envoy。

6. **Gateway vs VirtualService 的职责分工**：`Gateway` 配置 IngressGateway 监听哪些端口和协议（相当于 Nginx `server` 块）；`VirtualService` 配置路由规则（相当于 Nginx `location` 块）。两者通过 `gateways` 字段绑定，Gateway 的 `selector.istio: ingressgateway` 标签决定规则应用到哪个 Envoy 实例。

---

## §01 Istio 组件概览

### 官方架构

Istio 服务网格分为数据平面和控制平面：

- **数据平面**：由一组 Envoy sidecar 代理组成，负责协调和控制微服务间的所有网络通信，同时收集遥测数据
- **控制平面**：istiod，管理并配置代理进行流量路由

```
┌──────────────────────────────────────────────────────────┐
│  Istio Mesh (Data plane)                                  │
│                                                           │
│  ┌─────────────────┐    Mesh traffic   ┌───────────────┐ │
│  │  Service A       │ ──────────────→  │  Service B     │ │
│  │  ┌───────────┐  │                  │  ┌──────────┐  │ │
│  │  │   Proxy   │  │                  │  │  Proxy   │  │ │
│  └──┴───────────┴──┘                  └──┴──────────┴──┘ │
│         ▲  ▲                                 ▲  ▲        │
│         │  │ (Discovery/Config/Certs)         │  │        │
└─────────│──┼──────────────────────────────────│──┼────────┘
          │  └──────────────┬───────────────────┘  │
          └─────────────────┘                       │
                            ▼
┌──────────────────────────────────────────────────────────┐
│  Control plane                                            │
│  ┌───────────────────────────────────────────────────┐   │
│  │  istiod   │  Pilot  │  Citadel  │  Galley         │   │
│  └───────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

### 实际部署的 pod（Istio 1.11.4）

```bash
kubectl get pod -A | grep -i istio
# istio-system   istio-egressgateway-*
# istio-system   istio-ingressgateway-*
# istio-system   istiod-*
# 可观测性：prometheus, kiali, jaeger, grafana（另装）
```

官方文档中的 `galley` 在 1.11.4 已合并进 istiod，不单独部署。

### Envoy 的作用（Istio 扩展版）

Istio 使用 Envoy 的扩展版本（`proxyv2`），作为 sidecar 在逻辑上为服务增加了：

- 动态服务发现、负载均衡、TLS 终端
- HTTP/2 与 gRPC 代理、熔断器
- 健康检查、基于百分比的分阶段发布
- 故障注入、丰富的指标

### istiod 的作用

istiod 主要由三部分组成：

- **Pilot**：提炼特定平台（Kubernetes、Consul、MCP）的服务发现机制，整合为标准格式，任何符合 Envoy API 的 sidecar 均可使用
- **Citadel**：证书管理，通过 SDS 向 Envoy 下发 workload TLS 证书
- **Galley**：配置验证，将 Istio CRD 翻译为 Envoy 可理解的 xDS 配置

### istio-ingressgateway 解析

```
istio-ingressgateway
  └── deployment: istio-ingressgateway
      └── image: docker.io/istio/proxyv2:1.11.4
          └── args: proxy router
                    --domain $(POD_NAMESPACE).svc.cluster.local
                    --proxyLogLevel=warning
```

| 层级 | 说明 |
|------|------|
| NodePort Service | 对外暴露 15021/80/443 等端口 |
| Pod | 运行 pilot-agent + envoy |
| Gateway CRD | 声明监听规则，selector 绑定到该 pod |
| VirtualService CRD | 声明路由规则，gateways 字段引用 Gateway |

**bookinfo-gateway 示例**：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: Gateway
metadata:
  name: bookinfo-gateway
spec:
  selector:
    istio: ingressgateway   # 绑定到有此标签的 Envoy pod
  servers:
  - port:
      number: 80
      name: http
      protocol: HTTP
    hosts:
    - "*"
---
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: bookinfo
spec:
  hosts:
  - "*"
  gateways:
  - bookinfo-gateway
  http:
  - match:
    - uri:
        exact: /productpage
    - uri:
        prefix: /static
    route:
    - destination:
        host: productpage
        port:
          number: 9080
```

### Dockerfile 中的四个步骤（proxyv2 镜像）

```dockerfile
# 01 以 distroless 为基础（不含 shell），适合生产
# 02 安装 Envoy 二进制
COPY ${TARGETARCH}/envoy /usr/local/bin/${SIDECAR}
# 03 安装初始化脚本 pilot-agent（引导 Envoy）
COPY ${TARGETARCH}/pilot-agent /usr/local/bin/pilot-agent
# 04 pilot-agent 会 bootstrap Envoy
ENTRYPOINT ["/usr/local/bin/pilot-agent"]
```

---

## §02 xDS 协议介绍

### 什么是 xDS

xDS 是 "X Discovery Service" 的总称，"X" 代表不同资源类型。xDS 协议基于 gRPC 实现，Envoy 通过 gRPC streaming 订阅 Pilot 的资源配置。

### xDS API 全表

| 服务简写 | 全名 | 描述 |
|---------|------|------|
| LDS | Listener Discovery Service | 监听器发现服务 |
| RDS | Route Discovery Service | 路由发现服务 |
| CDS | Cluster Discovery Service | 集群发现服务 |
| EDS | Endpoint Discovery Service | 端点发现服务（旧名为 SDS） |
| SDS | Secret Discovery Service | 密钥/证书发现服务 |
| ADS | Aggregated Discovery Service | 聚合发现服务 |
| VHDS | Virtual Host Discovery Service | 虚拟主机发现服务 |
| HDS | Health Discovery Service | 健康发现服务 |
| RLS | Rate Limit Service | 限流发现服务 |
| MS | Metric Service | 指标服务 |

### Pilot 内部的 ADS 架构

```
Kubernetes ─┐
Consul     ─┼─→ Platform Adapter ─→ Aggregator Registry ─→ ADS Server
MCP        ─┘                                                    │
                                                         gRPC streaming
                                                         ┌───┬───┬───┐
                                                         │ E │ E │ E │  (Envoy)
                                                         └───┴───┴───┘
```

Pilot 借助 ADS 对 API 更新推送排序的能力，**按照 CDS-EDS-LDS-RDS 的顺序**串行分发配置，避免 Envoy 收到 Listener 时对应 Cluster 尚未就绪。

### xDS 的推和拉

**全量 vs 增量**：

- **SotW（State of the World）**：全量传输，每次响应包含完整资源列表
- **增量（Incremental）**：只传输变化部分，适合大规模集群

**每个资源独立是资源聚合**：

| 变量 | 说明 |
|------|------|
| State of the World（Basic xDS）| 全量 gRPC stream |
| Incremental xDS | 增量 gRPC stream |
| Aggregated Discovery Service（ADS）| 聚合全量 gRPC stream |
| Incremental ADS | 聚合增量 gRPC stream（暂未实现）|

### 最终一致性保证

Envoy xDS API 最终一致——整体而言，做到了先一步让系统收敛的最佳努力。在 CDS/EDS 更新之后，RDS 更新之前，Envoy 会临时持有不一致状态，但最终所有更新到达后系统收敛，流量路由正确。

---

## §03 istio-ingressgateway 与 istio-proxy 中的 pilot-agent 分析

### pilot-agent 的职责

| 角色 | 说明 |
|------|------|
| istio-ingressgateway | 1 个 Envoy pod，启动参数 `proxy router`，作为 Gateway |
| istio-proxy sidecar | 业务 pod 旁注入，启动参数 `proxy sidecar` |

两种角色底层完全一样，都是 pilot-agent 启动 Envoy，区别仅在 pilot-agent 传给 Envoy 的配置和标识。

### pilot-agent 启动流程

入口：`pilot/cmd/pilot-agent/main.go`

```
main.go
  └── rootCmd (cobra.Command)
        └── proxyCmd (proxy 子命令)
              └── init() 中 rootCmd.AddCommand(proxyCmd)
                    └── RunE → agent.Run(ctx)
```

`agent.Run` 中的核心步骤：

```
agent.Run(ctx)
  │
  ├── 01 启动 SDS gRPC server
  │     sds.NewServer() → 监听 Unix socket
  │     负责向 Envoy 下发 workload 证书（从 Kubernetes secret 获取）
  │
  ├── 02 启动 xDS gRPC server（XdsProxy）
  │     initXdsProxy(a)
  │       └── proxy.initDownstreamServer()
  │             └── RegisterAggregatedDiscoveryServiceServer(grpc, p)
  │     作为代理：下游接收 Envoy 的 ADS 请求，上游转发给 istiod
  │
  ├── 03 初始化 Envoy 对象
  │     a.initializeEnvoyAgent(ctx)
  │
  └── 04 启动 Envoy 进程
        a.proxy.Run(epoch, abortCh)
          └── exec.Command(e.BinaryPath, args...)
                cmd.Start() → 守护 Envoy 进程
```

### SDS server 的意义

SDS server 最重要的功能是**从 Kubernetes secret 中读取证书并动态下发给 Envoy**：

- Envoy 启动时如果 SDS server 不 active，listener 不会 active，流量无法进入
- 证书轮换时，SDS server 通知 Envoy 热更新证书，不需要重启

### XdsProxy 的数据流

```
Envoy (downstream)
    │  gRPC DeltaDiscoveryRequest
    ▼
XdsProxy.handleUpstreamDeltaRequest
    │  放入 p.connected.deltaRequestsChan
    ▼
HandleDeltaUpstream
    ├── handleUpstreamDeltaRequest  (发送到 istiod)
    └── handleUpstreamDeltaResponse (接收 istiod 响应，ACK/NACK 回 Envoy)
```

关键函数调用链（`pkg/istio-agent/agent.go`）：

```go
// 01 健康检查轮询，构造 DiscoveryRequest 和 DeltaDiscoveryRequest 并行
proxy.PerformApplicationHealthCheck(...)

// 02 DeltaDiscoveryRequest 入队
func (p *XdsProxy) PersistDeltaRequest(req *discovery.DeltaDiscoveryRequest) {
    ch := p.connected.deltaRequestsChan
    // 放入 channel，HandleDeltaUpstream goroutine 消费
}

// 03 上游发送
func (p *XdsProxy) sendUpstreamDelta(deltaUpstream ...) error {
    return istigrpc.Send(deltaUpstream.Context(), func() error {
        return deltaUpstream.Send(req)
    })
}

// 04 接收响应后 forwardDeltaToEnvoy
func (p *XdsProxy) handleUpstreamDeltaResponse(con *ProxyConnection) {
    for {
        select {
        case resp := <-con.deltaResponsesChan:
            // 根据 TypeUrl 路由处理
            // 发送 ACK/NACK 回 Envoy
            con.sendDeltaRequest(&discovery.DeltaDiscoveryRequest{
                TypeUrl:       resp.TypeUrl,
                ResponseNonce: resp.Nonce,
                ErrorDetail:   errorResp,
            })
        }
    }
}
```

### istio-ingressgateway 与 istio-proxy sidecar 的区别

通过查看各自 Envoy 的 admin 配置（`curl localhost:15000/config_dump`）：

- **ingressgateway**：`dynamic_route_configs` 中有 `route_config.name: "http.8080"`，路由规则来自 `bookinfo-gateway` → `productpage:9080`
- **sidecar**：`outbound|80||reviews.default.svc.cluster.local` 等 outbound cluster，为服务间通信服务

**VirtualService 权重路由示例**（50% 流量到 reviews:v3）：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: reviews
spec:
  hosts:
  - reviews
  http:
  - route:
    - destination:
        host: reviews
        subset: v1
      weight: 50
    - destination:
        host: reviews
        subset: v3
      weight: 50
```

对应 Envoy 中 `outbound|80||reviews.default.svc.cluster.local` cluster 的 `route_config` 会有两个 weighted cluster，各 50% 权重——VirtualService 的抽象最终落地为 Envoy 原生的 route 配置。

---

## §04 istiod pod 对应的 pilot-discovery 分析

### istiod 的启动入口

| 读码目标 | 源文件 | 入口函数 |
|---------|--------|---------|
| cobra 根命令 | `pilot/cmd/pilot-discovery/main.go` | `newDiscoveryCommand` |
| Server 构造 | `pilot/pkg/bootstrap/server.go` | `NewServer` |
| Controller 初始化 | `pilot/pkg/bootstrap/server.go` | `initControllers` |
| CRD controller | `pilot/pkg/config/kube/crdclient/client.go` | `New` |
| CRD schema 注册 | `pilot/pkg/config/schema/collections/collections.gen.go` | `Pilot` |
| 服务变更处理 | `pilot/pkg/serviceregistry/serviceentry/servicediscovery.go` | `serviceEntryHandler` |

### 启动流程总览

```
pilot-discovery main.go
  └── newDiscoveryCommand (cobra)
        └── RunE
              ├── bootstrap.NewServer(serverArgs)
              │     ├── initControllers(args)       // 初始化4个 controller
              │     │     ├── initMulticluster
              │     │     ├── initCertController    // 证书
              │     │     ├── initConfigController  // CRD 配置
              │     │     └── initServiceControllers// Service/Endpoint
              │     └── addStartFunc(...)           // 注册启动函数到 i.components
              │
              ├── discoveryServer.Start(stop)
              │     └── s.server.Start(stop)        // 遍历 i.components，逐一启动
              │           └── 启动 ADS gRPC server
              │
              └── discoveryServer.WaitUntilCompletion()
```

### 02 SidecarInjector 注入准入控制插件

istiod 内嵌 MutatingAdmissionWebhook（在第39章已详解）：

```go
s.initSidecarInjector(args)    // 注入 webhook
s.initConfigValidation(args)   // 配置校验 webhook
```

### 03 启动 ADS gRPC server

入口 `initDiscoveryService`：

```go
s.initDiscoveryService(args)
  └── s.addStartFunc(func(stop <-chan struct{}) error {
          s.XDSServer.Start(stop)   // 启动 ADS server
      })
```

`addStartFunc` 将 fn 放入 `i.components` channel；`server.Start()` 遍历该 channel，逐一调用 component 的 `Start` 方法——这是 istiod 的组件启动框架。

### 04 CRD controller（initConfigController）

```go
func (s *Server) initConfigController(args *PilotArgs) error {
    // 核心：initK8SConfigStore
    s.initK8SConfigStore(args)
      └── s.makeKubeConfigController(args)
            └── crdclient.New(s.kubeClient, args.Revision,
                             args.RegistryOptions.KubeOptions.DomainSuffix)
}
```

`crdclient.New` 中通过 `collections.Pilot` 获取所有 Istio CRD schema：

```go
// collections.gen.go
Pilot = collection.NewSchemasBuilder().
    MustAdd(IstioExtensionsV1Alpha1Wasmplugins).
    MustAdd(IstioNetworkingV1Alpha3DestinationRules).
    MustAdd(IstioNetworkingV1Alpha3Gateways).
    MustAdd(IstioNetworkingV1Alpha3VirtualServices).
    // ... 更多 CRD
    MustBuild()
```

**Gateway schema 注册示例**：

```go
IstioNetworkingV1Alpha3Gateways = collection.Builder{
    Name:     "istio/networking/v1alpha3/gateways",
    Resource: resource.Builder{
        Group:   "networking.istio.io",
        Kind:    "Gateway",
        Version: "v1alpha3",
        // ...
        Validate: validation.ValidateGateway,
    }.MustBuild(),
}.MustBuild()
```

### CRD 事件 → xDS 推送链路

```
K8s CRD 变更（Gateway/VirtualService/...）
  │
  ▼ Informer AddEventHandler (createCacheHandler)
  │   AddFunc / UpdateFunc / DeleteFunc
  │
  ▼ cl.queue.Push(h.onEvent)
  │
  ▼ worker 消费队列
  │   handleCRDAdd / handleCRDUpdate
  │     └── 创建对应 resourceGVK 的 informer（Gateway/VirtualService 各自的）
  │
  ▼ createCacheHandler（每种 CRD 注册回调）
  │   i.Informer().AddEventHandler({
  │       AddFunc:    cl.queue.Push(h.onEvent(nil, obj, EventAdd))
  │       UpdateFunc: cl.queue.Push(h.onEvent(old, cur, EventUpdate))
  │       DeleteFunc: cl.queue.Push(h.onEvent(nil, obj, EventDelete))
  │   })
  │
  ▼ h.onEvent → RegisterEventHandler 注册的回调
  │   位于 serviceentry/servicediscovery.go
  │
  ▼ serviceEntryHandler
  │   switch event {
  │   case EventUpdate:
  │       // diff old/new service，分出 added/updated/deleted/unchanged
  │   case EventAdd:   addedSvcs = cs
  │   case EventDelete: deletedSvcs = cs
  │   }
  │   for _, svc := range addedSvcs {
  │       s.XdsUpdater.SvcUpdate(shard, svc.Hostname, svc.Namespace, ...)
  │   }
  │
  ▼ XdsUpdater.SvcUpdate → 触发增量 xDS 推送给所有连接的 Envoy
```

**`createCacheHandler` 关键代码**（`pilot/pkg/config/kube/crdclient/client.go`）：

```go
func createCacheHandler(cl *Client, schema collection.Schema,
                         i informers.GenericInformer) *cacheHandler {
    h := &cacheHandler{client: cl, schema: schema, informer: i.Informer()}
    // lister 按 namespace 或全局
    h.lister = func(namespace string) cache.GenericNamespaceLister {
        if schema.Resource().IsClusterScoped() {
            return i.Lister()
        }
        return i.Lister().ByNamespace(namespace)
    }
    // 注册三类事件
    i.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            incrementEvent(kind, "add")
            cl.queue.Push(func() error {
                return h.onEvent(nil, obj, model.EventAdd)
            })
        },
        UpdateFunc: func(old, cur interface{}) {
            incrementEvent(kind, "update")
            cl.queue.Push(func() error {
                return h.onEvent(old, cur, model.EventUpdate)
            })
        },
        DeleteFunc: func(obj interface{}) {
            incrementEvent(kind, "delete")
            cl.queue.Push(func() error {
                return h.onEvent(nil, obj, model.EventDelete)
            })
        },
    })
    return h
}
```

这里的 `queue` 是 Kubernetes 标准的 `workqueue`，与 kube-controller-manager 中 ReplicaSetController 的 worker 模式完全一致——**Istio CRD controller 与 K8s 内置 controller 使用同一套 Informer + Queue + Worker 模式**。
