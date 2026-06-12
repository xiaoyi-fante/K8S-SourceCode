# 第31章 k8s Ingress 7层路由机制和 traefik 源码解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 31 章 — k8s Ingress 7层路由机制和traefik源码解读
> **源码入口**: `cmd/traefik/traefik.go → main()`（traefik 仓库，非 kubernetes 仓库）

---

## 核心机制一览

1. **Ingress Controller 是独立进程**：Ingress 对象本身只是 API Server 中的一条路由规则声明，必须有 Ingress Controller 进程将这些规则同步到实际的反向代理（nginx / traefik 等）。Ingress Controller 不内置在 kube-controller-manager 中，需单独部署。

2. **traefik = ingress controller + 反向代理二合一**：traefik 自身既是 HTTP server（处理用户请求，按 Ingress rule 转发到后端 svc），也是 Kubernetes provider（监听 Ingress/Service/Endpoint 变化，动态更新路由）。不需要像 nginx-ingress 那样 reload 配置文件。

3. **生产者-消费者双通道模型**：traefik 内部用两个 channel 解耦配置变更的生产和消费：`configurationChan`（原始事件）→ `configurationValidatedChan`（经 throttle 去抖后的有效配置）→ `loadMessage` 遍历 Listeners 执行热更新。整条链路异步，反向代理本身不停机。

4. **两类 Listener 驱动热更新**：`setupServer` 注册两个 Listener 回调：① `ServerTransports`（更新 traefik 与后端 upstream 之间的 HTTP transport 连接参数）；② `switchRouter`（原子替换路由 map，`serverEntryPoints.Switch(routers)` 切换生效）。收到新配置时两者均被触发。

5. **kubernetes provider 的 Informer 架构**：kubernetes ingress provider 通过 `k8sClient.WatchAll` 为 Ingress / Service / Secret 各自创建 SharedInformer，事件通过 `eventsChan` 汇集，经限流（throttle）后发往 `configurationChan`，实现对 apiserver 的 watch-based 感知，而非轮询。

---

## 全章调用链总图

```
traefik 启动
  │
  ▼ main() — cmd/traefik/traefik.go
  │  初始化配置 NewTraefikConfiguration
  │  构建 loaders (FileLoader / FlagLoader / EnvLoader)
  │
  ▼ runCmd(staticConfiguration)
  │
  ▼ setupServer(staticConfiguration)
  │  ├─── NewConfigurationWatcher(...)       ← 创建 Watcher，持有 configurationChan
  │  ├─── watcher.AddListener(ServerTransports)  ← Listener 1：更新 HTTP Transport
  │  └─── watcher.AddListener(switchRouter)      ← Listener 2：切换路由表
  │
  ▼ svr.Start(ctx)
  │  ├─── s.entryPointsStart()
  │  ├─── s.udpEntryPointsStart()
  │  └─── s.routinesPool.GoCtx(s.listenSignals)
  │
  ▼ watcher.Start()
  │  ├─── startProvider()                  ← 启动各 provider goroutine
  │  ├─── listenProviders()                ← 消费 configurationChan
  │  └─── listenConfigurations()           ← 消费 configurationValidatedChan → loadMessage

kubernetes ingress provider 内部
  │
  ▼ Provider.Provide(configurationChan, pool)
  │  ├─── p.newK8sClient()                 ← 创建 k8sClient
  │  └─── k8sClient.WatchAll(namespaces)   ← 启动 Informers
  │         ├─── factoryIngress (Ingress Informer)
  │         ├─── factoryKube   (Service / Endpoints Informer)
  │         └─── factorySecret (Secret Informer)
  │
  ▼ eventsChan (汇总所有 Informer 事件)
  │
  ▼ buildConfiguration(client) → conf
  │
  ▼ configurationChan <- dynamic.Message{ProviderName: "kubernetes", Configuration: conf}
```

---

## §01 Ingress 安装使用

### Ingress 是什么

Ingress 是 Kubernetes 提供的**七层路由入口**，位于 Service 之上。它根据 HTTP 请求的 Host 和 URL Path 将流量路由到不同的后端 Service，相当于集群级别的反向代理 / 负载均衡器。

```
互联网流量
  │
  ▼ Ingress（L7：host/path 路由）
  │
  ├─── api.example.com       → svc-api
  ├─── example.com/web       → svc-web
  └─── backoffice.example.com → svc-backoffice
```

**Ingress vs 自己起动 nginx pod 的区别**：

- `ingress = nginx + 服务发现`
- Ingress Controller 可以理解为一个监听器，不断地与 kube-apiserver 交流，实时感知后端 Service 和 Pod 的变化；当变化发生后，Ingress Controller 再结合 Ingress 配置更新反向代理负载均衡，达到服务发现的效果。这与 consul + consul-template 的模式非常类似。

### Ingress Controller 选择

必须有 Ingress Controller 运行，Ingress 对象才能生效。与其他类型的控制器不同，Ingress Controller **不**作为 kube-controller-manager 的一部分运行，需要单独部署，并选择最适合集群的实现：

| Controller | 特点 |
|-----------|------|
| traefik | 配置简单，自动动态配置，与微服务系统集成好 |
| nginx-ingress-controller | 性能更强，功能更丰富，但配置复杂 |
| Kong Ingress Controller | API Gateway 功能 |
| HAProxy Ingress Controller | 高性能 HAProxy |

本章以 **traefik** 为例，它比 nginx-controller 配置更简单。

### 部署 traefik（步骤概览）

**第一步：部署 RBAC**

traefik 需要 watch Ingress / Service / Endpoints / Secrets，因此需要相应的 ClusterRole 权限：

```yaml
# rbac.yaml（核心权限）
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: traefik-ingress-controller
rules:
  - apiGroups: [""]
    resources: [services, endpoints, secrets]
    verbs: [get, list, watch]
  - apiGroups: [extensions]
    resources: [ingresses]
    verbs: [get, list, watch]
---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: traefik-ingress-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: traefik-ingress-controller
subjects:
  - kind: ServiceAccount
    name: traefik-ingress-controller
    namespace: kube-system
```

**第二步：部署 traefik Deployment**

```yaml
# traefik.yaml（关键字段）
kind: Deployment
apiVersion: apps/v1
metadata:
  name: traefik-ingress-controller
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      k8s-app: traefik-ingress-lb
  template:
    spec:
      serviceAccountName: traefik-ingress-controller
      terminationGracePeriodSeconds: 60
      containers:
        - image: traefik:v1.7.17
          name: traefik-ingress-lb
          ports:
            - name: http
              containerPort: 80      # 业务流量入口
            - name: admin
              containerPort: 8080    # traefik Dashboard UI
          args:
            - --api
            - --kubernetes
            - --logLevel=INFO
---
# traefik 对外暴露的 Service
kind: Service
apiVersion: v1
metadata:
  name: traefik-ingress-service
  namespace: kube-system
spec:
  selector:
    k8s-app: traefik-ingress-lb
  ports:
    - protocol: TCP
      port: 80
      name: web
      nodePort: 801    # ingress 入口，业务请求从此进入
    - protocol: TCP
      port: 8080
      name: admin
      nodePort: 8080   # traefik Dashboard
  type: NodePort
```

> traefik Dashboard（8080 端口）也可以通过 Ingress 来访问，避免直接暴露 NodePort。

**第三步：创建 Ingress 对象**

以将 traefik Dashboard 通过 Ingress 暴露为例：

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: traefik-web-ui
  namespace: kube-system
  annotations:
    kubernetes.io/ingress.class: traefik   # 指定使用 traefik controller
spec:
  rules:
    - host: traefik.xiaoyi.com
      http:
        paths:
          - pathType: Prefix
            path: "/"
            backend:
              service:
                name: traefik-ingress-service  # 指向 traefik-ui 对应的 svc
                port:
                  name: admin                  # 对应 8080 端口
```

在笔记本 `/etc/hosts`（Windows: `C:\Windows\System32\drivers\etc\hosts`）添加：
```
<node-ip>  traefik.xiaoyi.com
```

之后访问 `http://traefik.xiaoyi.com:801` 即可看到 traefik Dashboard。
- 注意 801 端口对应的是 `traefik-ingress-service` 这个 svc 的 80 端口，也就是 ingress 的入口。

**第四步：创建业务 nginx svc 和 Ingress 规则**

```yaml
# nginx 业务应用
kind: Deployment
apiVersion: apps/v1
metadata:
  name: nginx-svc09
  namespace: kube-system
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nginx-svc09
  template:
    spec:
      containers:
        - name: nginx
          image: nginx:1.8
          ports:
            - containerPort: 80
---
kind: Service
apiVersion: v1
metadata:
  name: nginx-svc09
  namespace: kube-system
spec:
  selector:
    app: nginx-svc09
  ports:
    - protocol: TCP
      name: index
      port: 80
      targetPort: 80
---
# 修改 Ingress 规则，增加 nginx 路由
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: traefik-web-ui
  namespace: kube-system
  annotations:
    kubernetes.io/ingress.class: traefik
spec:
  rules:
    - host: traefik.xiaoyi.com
      http:
        paths:
          - pathType: Prefix
            path: "/"
            backend:
              service:
                name: traefik-ingress-service
                port:
                  name: admin
    - host: abc.xiaoyi.com             # 新增路由
      http:
        paths:
          - pathType: Prefix
            path: "/"
            backend:
              service:
                name: nginx-svc09      # 指向 nginx svc
                port:
                  name: index
```

访问 `http://abc.xiaoyi.com:801` → traefik 按 Host 路由到 nginx-svc09 → 转发给 nginx pod。

---

## §02 traefik 源码解读

本节分析 traefik 的 kubernetes ingress provider 模式，重点是：traefik 如何感知 Ingress 规则变化，并动态更新路由，而无需重启或 reload。

traefik 提供 k8s provider 和 CRD 两种模式，本节分析 k8s provider 模式。

### 总体架构：生产者-消费者

traefik 内部用 channel 驱动配置热更新，分为三个角色：

```
【生产者】kubernetes ingress provider
  │  watch Ingress/Service/Endpoints Informer
  │  事件汇入 eventsChan → buildConfiguration → configurationChan
  │
  ▼ configurationChan（chan dynamic.Message）
  │
【中间层】ConfigurationWatcher.listenProviders
  │  throttle 限流（providersThrottleDuration）
  │  → configurationValidatedChan
  │
  ▼ configurationValidatedChan（chan dynamic.Message）
  │
【消费者】ConfigurationWatcher.listenConfigurations
  │  → preLoadConfiguration（过滤空配置、throttle）
  │  → providerConfigUpdateCh
  │  → throttleProviderConfigReload
  │  → configurationValidatedChan
  │
  ▼ loadMessage(configMsg)
     遍历 c.configurationListeners（回调列表）
     → Listener 1: ServerTransports.Update(conf)   更新 upstream HTTP transport
     → Listener 2: switchRouter(conf)               原子替换路由表
```

### 启动入口

**文件**：`cmd/traefik/traefik.go`

```go
func main() {
    // traefik config 初始化
    tConfig := cmd.NewTraefikConfiguration()

    loaders := []cli.ResourceLoader{&tcli.FileLoader{}, &tcli.FlagLoader{}, &tcli.EnvLoader{}}

    cmdTraefik := &cli.Command{
        Name: "traefik",
        Run: func(_ []string) error {
            return runCmd(&tConfig.Configuration)
        },
    }
    // ...
}
```

`runCmd` 只做一件事：调用 `setupServer`。

```go
func runCmd(staticConfiguration *static.Configuration) error {
    svr, err := setupServer(staticConfiguration)
    // ...
}
```

### setupServer：构建 Server 和 Watcher

```go
func setupServer(staticConfiguration *static.Configuration) (*server.Server, error) {
    // 创建 Watcher，持有 configurationChan
    watcher := server.NewConfigurationWatcher(
        routinesPool,
        providerAggregator,
        time.Duration(staticConfiguration.Providers.ProvidersThrottleDuration),
        getDefaultsEntrypoints(staticConfiguration),
        "internal",
    )

    // Listener 1：更新 traefik 与后端 upstream 之间的 HTTP Transport
    watcher.AddListener(func(conf dynamic.Configuration) {
        // Server Transports
        roundTripperManager.Update(conf.HTTP.ServersTransports)
    })

    // Listener 2：切换路由表
    watcher.AddListener(switchRouter(routerFactory, serverEntryPointsTCP, serverEntryPointsUDP))

    // 启动 svr
    // svr.Start(ctx) 内部调用 entryPointsStart + udpEntryPointsStart + listenSignals
    // watcher.Start() 内部启动 startProvider + listenProviders + listenConfigurations
}
```

**`ConfigurationWatcher` 结构**（`pkg/server/configurationwatcher.go`）：

```go
type ConfigurationWatcher struct {
    provider                  provider.Provider
    defaultEntryPoints        []string
    providersThrottleDuration time.Duration
    currentConfigurations     safe.Safe

    configurationChan          chan dynamic.Message   // 原始事件
    configurationValidatedChan chan dynamic.Message   // throttle 后的有效配置
    providerConfigUpdateMap    map[string]chan dynamic.Message

    requiredProvider       string
    configurationListeners []func(dynamic.Configuration)  // Listener 回调列表

    routinesPool *safe.Pool
}
```

`configurationChan` 是整个热更新的核心通道，生产者往里写，消费者从里读。

### Listener 1：ServerTransports（更新 upstream 连接参数）

```go
// setupServer 中注册：
watcher.AddListener(func(conf dynamic.Configuration) {
    // roundTripperManager.Update 更新 configName → RoundTripper 的 map
    roundTripperManager.Update(conf.HTTP.ServersTransports)
})
```

`roundTripperManager.Update` 内部逻辑：
- 遍历旧 config，删除已不存在的 RoundTripper
- 遍历新 config，为新增的 configName 调用 `createRoundTripper(newConfig)` 创建 HTTP Transport
- 更新失败时 fallback 到 `http.DefaultTransport`

这个 Listener 负责管理 traefik 到后端 svc 的 HTTP 连接池参数（超时、TLS、CA 等），保证路由切换时连接参数同步更新。

### Listener 2：switchRouter（原子替换路由表）

```go
// switchRouter 返回一个闭包，作为 Listener 注册
func switchRouter(routerFactory *server.RouterFactory,
    serverEntryPointsTCP server.TCPEntryPoints,
    serverEntryPointsUDP server.UDPEntryPoints) func(dynamic.Configuration) {
    return func(conf dynamic.Configuration) {
        rtConf := runtime.NewConfig(conf)
        routers, udpRouters := routerFactory.CreateRouters(rtConf)

        if aviator != nil {
            aviator.SetDynamicConfiguration(conf)
        }

        serverEntryPointsTCP.Switch(routers)      // 原子替换 TCP 路由表
        serverEntryPointsUDP.Switch(udpRouters)   // 原子替换 UDP 路由表
    }
}
```

每次收到新的 Ingress 配置时，`routerFactory.CreateRouters` 重新构建整张路由 map，然后通过 `Switch` 原子切换，不需要 reload 或重启。

### svr.Start(ctx)：启动服务和 Watcher

```go
// pkg/server/server.go
func (s *Server) Start(ctx context.Context) {
    go func() {
        // 服务启动
        s.entryPointsStart.Start(stopCh)
        s.udpEntryPoints.Start(stopCh)
        s.routinesPool.GoCtx(s.listenSignals)
    }()
}
```

`watcher.Start()` 内部启动三个 goroutine：
1. `startProvider()` — 启动各 provider（包括 kubernetes ingress provider）
2. `listenProviders()` — 消费 `configurationChan`，throttle 后写入 `configurationValidatedChan`
3. `listenConfigurations()` — 消费 `configurationValidatedChan`，调用 `loadMessage`

```go
func (c *ConfigurationWatcher) startProvider() {
    // ...
    currentProvider := c.provider
    safe.Go(func() {
        err := currentProvider.Provide(c.configurationChan, c.routinesPool)
    })
}
```

### kubernetes ingress provider：感知 Ingress 变化

**文件**：`pkg/provider/kubernetes/ingress/kubernetes.go`

```go
func (p *Provider) Provide(configurationChan chan<- dynamic.Message, pool *safe.Pool) error {
    // 创建 k8s client
    k8sClient, err := p.newK8sClient(ctxLog)

    // 启动 Informers，监听所有 namespace
    eventsChan, err := k8sClient.WatchAll(p.Namespaces, ctxPool.Done())
    // ...

    // 事件循环
    for {
        select {
        case <-ctxPool.Done():
            return nil
        case event := <-eventsChan:
            // throttle：第一个事件立即处理，后续在窗口期内去抖
            conf := p.loadConfigurationFromIngress(ctxLog, k8sClient)
            configurationChan <- dynamic.Message{
                ProviderName:  "kubernetes",
                Configuration: conf,
            }
        }
    }
}
```

`k8sClient.WatchAll` 内部为每类资源创建 SharedInformer，并统一汇入 `eventsChan`：

```go
// WatchAll 内部（简化）
for _, ns := range namespaces {
    factoryIngress = informers.NewSharedInformerFactoryWithOptions(...)
    factoryIngress.Networking().V1().Ingresses().Informer().AddEventHandler(eventHandler)

    factoryKube = informers.NewSharedInformerFactoryWithOptions(...)
    factoryKube.Core().V1().Services().Informer().AddEventHandler(eventHandler)
    factoryKube.Core().V1().Endpoints().Informer().AddEventHandler(eventHandler)

    factorySecret = informers.NewSharedInformerFactoryWithOptions(...)
    factorySecret.Core().V1().Secrets().Informer().AddEventHandler(eventHandler)
}

// eventHandler 把所有事件塞入同一个 eventsChan
type ResourceEventHandler struct {
    ev chan<- interface{}
}
func (reh *ResourceEventHandler) OnAdd(obj interface{}) {
    eventHandlerFunc(reh.ev, obj)
}
```

三类 Informer（Ingress / Service+Endpoints / Secret）事件统一汇入 `eventsChan`，任何一类资源变化都触发 `buildConfiguration` 重新计算路由配置。

### 消费链路：configurationChan → loadMessage

```
Provider 写入 configurationChan
  │
  ▼ listenProviders()
  │  throttle（providersThrottleDuration 窗口内聚合）
  │  → providerConfigUpdateCh <- configMsg
  │
  ▼ throttleProviderConfigReload goroutine
  │  → configurationValidatedChan <- configMsg
  │
  ▼ listenConfigurations()
  │  case configMsg := <-c.configurationValidatedChan
  │  → c.preLoadConfiguration(configMsg)
  │
  ▼ preLoadConfiguration()
  │  深拷贝 conf，清理敏感字段（TLS Certificates / RootCAs）
  │  isEmptyConfiguration → 跳过
  │  throttleProviderConfigReload → providerConfigUpdateCh <- configMsg
  │
  ▼ configurationValidatedChan <- configMsg  （最终有效配置）
  │
  ▼ listenConfigurations() → c.loadMessage(configMsg)
  │
  ▼ loadMessage()
     for _, listener := range c.configurationListeners {
         listener(conf)    // 依次调用 Listener 1 和 Listener 2
     }
```

`loadMessage` 是整条链路的终点：它遍历 `setupServer` 时注册的所有 Listener 回调，依次执行 ServerTransports 更新和路由表切换，完成**零停机热更新**。

### 本节重点总结

**生产者**（kubernetes ingress provider）：
- 为 Ingress / Service / Endpoints / Secret 各创建 Informer，相当于实现了 controller 模式
- 事件塞入 `eventsChan`，经 throttle 限流后发往 `configurationChan`

**消费者**（ConfigurationWatcher）：
- `setupServer` 时注册两个 Listener 回调
- Listener 1（`ServerTransports`）：更新 traefik 与后端 upstream 之间的 HTTP Transport 连接对象
- Listener 2（`switchRouter`）：原子替换路由表

**HTTP server**：
- 负责处理用户请求，根据 Ingress rule 转发给后端众多 svc
- 等同于动态配置的反向代理 nginx，但无需 reload
