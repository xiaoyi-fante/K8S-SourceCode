# 第03章 apiserver 中的权限相关

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 03 章 — apiserver 中的权限相关
> **源码入口**: `cmd/kube-apiserver/apiserver.go`

---

## 核心机制一览

1. **三 server 委托链**：apiserver 启动时构造 aggregatorServer → kubeAPIServer → apiExtensionsServer 的委托链，请求从链头向下路由，各 server 只处理自己负责的 API group，未知路由自动下沉。

2. **buildGenericConfig 是权限体系的统一入口**：Authentication、Authorization、Audit、Admission 四大权限模块全部在 `buildGenericConfig` / `CreateKubeAPIServerConfig` 中用 `ApplyTo` 模式完成初始化，配置来源（命令行参数）与运行时结构完全解耦。

3. **Authentication union 短路**：多个认证器（x509、Token、Header 代理等）组成 union 链；某认证器返回 ok=true 立即短路返回，返回 error 则继续尝试下一个，全部失败才拒绝。

4. **Authorization union 按 Decision 短路**：鉴权链中只有 `DecisionAllow` 或 `DecisionDeny` 才短路，`DecisionNoOpinion`（未表态）才继续下一个鉴权器。与认证 union 的关键区别：error 不短路。

5. **Node 鉴权用内存图保证最小权限**：NodeAuthorizer 维护 Pod-Node-Volume 关系 graph，kubelet 只能访问与自身节点有 `hasPathFrom` 关系的 secret/configmap/pvc，从结构上防止 kubelet 跨节点读取敏感资源。

6. **RBAC 鉴权用 Visitor 模式解耦遍历与判断**：`VisitRulesFor` 负责遍历 ClusterRoleBinding + RoleBinding，`authorizingVisitor.visit` 负责判断，`allowed` 标志一旦置 true 立即停止遍历。所有规则通过 Informer 本地缓存获取，不走 etcd。

7. **Audit 三阶段事件投递**：审计 Backend 以中间件（`WithAudit`）形式注入 HTTP Handler 层，每个请求最多触发三次 `ProcessEvents`（请求收到、响应开始、响应完成），logBackend 和 webhookBackend 通过 `appendBackend` union 同时接收事件。

8. **Admission 两阶段 + 工厂注册**：所有准入插件以 `Factory func(config io.Reader) Interface` 注册在全局 `registry` map 中，`NewFromPlugins` 按序实例化并链式组装；先执行 Mutating 阶段（可修改对象），再执行 Validating 阶段（只读校验），两阶段严格隔离。

---

## 全章调用链总图

```
kube-apiserver 启动
  │
  ▼ main() → NewAPIServerCommand()                                  apiserver.go
  │   RunE: Complete → Validate → Run(completedOptions, stopCh)    server.go
  │
  ▼ CreateServerChain                                               server.go
  │   ├── createAPIExtensionsServer（CRD）← NewEmptyDelegate()
  │   ├── CreateKubeAPIServer（核心 API）← apiExtensionsServer
  │   └── createAggregatorServer（metrics）← kubeAPIServer          ← 返回链头
  │
  ▼ buildGenericConfig（§01-§02）                                   server.go
  │   ├── NewConfig(legacyscheme.Codecs)                           config.go
  │   ├── s.SecureServing.ApplyTo / s.Features.ApplyTo ...
  │   ├── NewStorageFactoryConfig → Complete → New                 （etcd 存储工厂）
  │   ├── LoopbackClientConfig 优化（protobuf + 禁压缩）
  │   ├── clientgoclientset.NewForConfig → versionedInformers
  │   │
  │   ├── s.Authentication.ApplyTo（§03）                          authentication.go
  │   │     └── authenticatorConfig.New()                          config.go
  │   │           ├── x509 certAuth
  │   │           ├── tokenunion.New(tokenAuthenticators...)
  │   │           └── union.New(authenticators...)  → unionAuthRequestHandler.AuthenticateRequest
  │   │
  │   ├── BuildAuthorizer（§04）                                    server.go
  │   │     └── authorizationConfig.New()                          config.go
  │   │           ├── modes.ModeNode → node.NewAuthorizer(graph)  （§05）
  │   │           │     └── Authorize: 4规则 → hasPathFrom(graph)
  │   │           ├── modes.ModeRBAC → rbac.New(Listers)          （§06）
  │   │           │     └── Authorize → VisitRulesFor
  │   │           │           ├── ClusterRoleBindings → appliesTo → GetRoleReferenceRules → visit
  │   │           │           └── RoleBindings        → appliesTo → GetRoleReferenceRules → visit
  │   │           └── union.New(authorizers...) → unionAuthzHandler.Authorize
  │   │
  │   └── s.AuditOptions.ApplyTo（§07）                            audit.go
  │         ├── newPolicyChecker（加载 audit-policy-file）
  │         ├── logBackend（写文件 / stdout）
  │         ├── webhookBackend → dynamicBackend（TruncateOptions 封装）
  │         └── c.AuditBackend = appendBackend(log, dynamic)
  │
  ▼ admissionConfig.New → pluginInitializers（§08）                config.go
  │   └── kubePluginInitializer（注入 RESTMapper/quota 等依赖）
  │
  ▼ s.Admission.ApplyTo                                            server.go
  │   ├── computePluginNames（推荐列表 + 开关计算）
  │   └── NewFromPlugins（按序实例化插件链）                         plugins.go
  │         └── InitPlugin → ps.getPlugin(registry) → Factory()
  │               └── pluginInitializer.Initialize(plugin)（注入依赖）
  │
  ▼ server.PrepareRun() → prepared.Run(stopCh)                     server.go
        └── 开始监听 HTTPS 请求
              │
              ▼ WithAudit（HTTP 中间件）→ ProcessEvents × 3       audit.go
              ▼ authentication → authorization → admission（Mutating → Validating）
              ▼ 写入 etcd
```

---

## §01 apiserver 启动主流程分析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| main 入口 | [apiserver.go](kubernetes/cmd/kube-apiserver/apiserver.go) | `main:35` |
| cobra 命令构造 + RunE | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `NewAPIServerCommand:105` |
| Run 函数 | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `Run:179` |
| 三条 server 链组装 | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `CreateServerChain:197` |
| 信号处理 stopCh | [signal.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/signal.go) | `SetupSignalHandler:33` |

本节从 `main()` 入口出发，走到 `CreateServerChain` 创建三个 server，再到 `PrepareRun().Run(stopCh)` 正式监听请求。

### 入口：main()

```go
// cmd/kube-apiserver/apiserver.go:35
func main() {
    rand.Seed(time.Now().UnixNano())
    pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
    command := app.NewAPIServerCommand()
    logs.InitLogs()
    defer logs.FlushLogs()
    if err := command.Execute(); err != nil {
        os.Exit(1)
    }
}
```

与 kubectl 的 `main()` 结构一致：构造 cobra.Command，调用 `Execute()`，错误时退出。

### NewAPIServerCommand：PersistentPreRunE + RunE

```go
// cmd/kube-apiserver/app/server.go:105
func NewAPIServerCommand() *cobra.Command {
    s := options.NewServerRunOptions()
    cmd := &cobra.Command{
        PersistentPreRunE: func(*cobra.Command, []string) error {
            // 静默 client-go loopback 告警，不影响功能
            rest.SetDefaultWarningHandler(rest.NoWarnings{})
            return nil
        },
        RunE: func(cmd *cobra.Command, args []string) error {
            verflag.PrintAndExitIfRequested()
            cliflag.PrintFlags(cmd.Flags())                    // 打印命令行参数
            err := checkNonZeroInsecurePort(cmd.Flags())      // 检查不安全端口
            completedOptions, err := Complete(s)               // 填充默认值
            if errs := completedOptions.Validate(); len(errs) != 0 {
                return utilerrors.NewAggregate(errs)           // 参数校验
            }
            return Run(completedOptions, genericapiserver.SetupSignalHandler())
        },
    }
    ...
}
```

`completedOptions` 是 `completedServerRunOptions`，内嵌 `*ServerRunOptions`，`Complete()` 完成所有字段的默认值填充后不再允许修改。

`Validate()` 是一个典型的 **聚合校验**：逐一调用各子模块（Authentication、Authorization、Audit、Metrics 等）的 Validate 方法，汇总所有错误后一次性返回，方便用户看到全部配置错误而不是每次只看到一个。

### stopCh：优雅停机信号

`genericapiserver.SetupSignalHandler()` 返回 `<-chan struct{}`，当进程收到 SIGTERM / SIGINT 时 close 这个 channel，通知所有持有它的 goroutine 开始退出。

```go
// staging/src/k8s.io/apiserver/pkg/server/signal.go:33
func SetupSignalHandler() <-chan struct{} {
    return SetupSignalContext().Done()
}

func SetupSignalContext() context.Context {
    close(onlyOneSignalHandler) // 保证只调用一次，二次调用直接 panic
    shutdownHandler = make(chan os.Signal, 2)
    ctx, cancel := context.WithCancel(context.Background())
    signal.Notify(shutdownHandler, shutdownSignals...)
    go func() {
        <-shutdownHandler
        cancel()          // 第一个信号：开始优雅退出
        <-shutdownHandler
        os.Exit(1)        // 第二个信号：强制退出
    }()
    return ctx
}
```

设计要点：两信号机制。第一个信号触发 context cancel，让 server 完成在途请求；第二个信号强制 `os.Exit(1)`，防止进程卡住。`onlyOneSignalHandler` 通道 close 作为 once 哨兵，比 `sync.Once` 更激进——重复调用直接 panic 而不是静默忽略，强制调用方保证只初始化一次。

### Run：三步走

```go
// cmd/kube-apiserver/app/server.go:179
func Run(completeOptions completedServerRunOptions, stopCh <-chan struct{}) error {
    server, err := CreateServerChain(completeOptions, stopCh)
    prepared, err := server.PrepareRun()
    return prepared.Run(stopCh)
}
```

三个动作严格串行：
1. `CreateServerChain` — 构造三个 server 并用 delegation 链串联
2. `PrepareRun` — 注册 health/readiness/metrics 路由等启动前准备
3. `Run(stopCh)` — 开始监听请求，直到 stopCh 关闭

### CreateServerChain：三个 server 的委托链

```go
// cmd/kube-apiserver/app/server.go:197
func CreateServerChain(completedOptions completedServerRunOptions, stopCh <-chan struct{}) (...) {
    kubeAPIServerConfig, serviceResolver, pluginInitializer, _ := CreateKubeAPIServerConfig(...)

    apiExtensionsConfig, _ := createAPIExtensionsConfig(...)
    apiExtensionsServer, _ := createAPIExtensionsServer(apiExtensionsConfig,
        genericapiserver.NewEmptyDelegate())      // 链尾：空委托

    kubeAPIServer, _ := CreateKubeAPIServer(kubeAPIServerConfig,
        apiExtensionsServer.GenericAPIServer)     // 委托给 apiExtensionsServer

    aggregatorConfig, _ := createAggregatorConfig(...)
    aggregatorServer, _ := createAggregatorServer(aggregatorConfig,
        kubeAPIServer.GenericAPIServer,           // 委托给 kubeAPIServer
        apiExtensionsServer.Informers)

    return aggregatorServer, nil                  // 返回链头
}
```

三个 server 通过 **delegation chain** 串联，请求路由按优先级从 aggregatorServer 向下查找：

```
请求入口
  │
  ▼ aggregatorServer（链头）        — 处理 metrics / APIServices
  │  委托给 ↓
  ▼ kubeAPIServer                   — 处理 Pod/Deployment/Service 等核心 API
  │  委托给 ↓
  ▼ apiExtensionsServer（链尾）     — 处理 CRD 自定义资源
  │  委托给 ↓
  ▼ NewEmptyDelegate()              — 空委托，路由终止
```

设计意图：三个 server 功能正交，用 delegation 而不是继承串联，让每个 server 只关注自己负责的 API group，未知路由自动下沉，不需要集中的路由注册表。

---

## §02 API核心服务通用配置genericConfig的准备工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| genericConfig 通用配置组装 | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `buildGenericConfig:442` |
| GenericConfig 结构定义 | [config.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/config.go) | `NewConfig:297` |

本节覆盖 `buildGenericConfig` 的核心步骤：`proxyTransport` 创建 → `genericConfig` 初始化 → 各 Options `ApplyTo` → etcd 存储工厂 → loopback 内部通信优化 → `clientset` 创建。

### 整体调用结构

```
CreateKubeAPIServerConfig
  │
  ▼ buildGenericConfig(s.ServerRunOptions, proxyTransport)
  │   ├── genericapiserver.NewConfig(legacyscheme.Codecs)      创建通用配置骨架
  │   ├── s.SecureServing.ApplyTo(...)                          HTTPS 端口/证书配置
  │   ├── s.Features.ApplyTo / s.EgressSelector.ApplyTo ...    各子选项应用
  │   ├── NewStorageFactoryConfig → Complete → New             etcd 存储工厂
  │   ├── s.Etcd.ApplyWithStorageFactoryTo(...)                 RESTOptionsGetter 注入
  │   ├── LoopbackClientConfig 内部通信优化                       protobuf + 禁用压缩
  │   ├── clientgoclientset.NewForConfig(loopback)              创建 clientset
  │   └── NewSharedInformerFactory(client, 10min)               versionedInformers
  │
  └── 返回: genericConfig, versionedInformers, serviceResolver, pluginInitializers, storageFactory
```

### proxyTransport：与节点通信的 HTTP 传输层

```go
proxyTransport := CreateProxyTransport()
```

`proxyTransport` 专门用于转发 `kubectl exec` / `logs` / `port-forward` 等需要与 kubelet 直连的请求，与 loopback 内部通信使用的 `LoopbackClientConfig` 是两套不同的 Transport。

### genericConfig 初始化与 ApplyTo 模式

```go
// cmd/kube-apiserver/app/server.go:454
genericConfig = genericapiserver.NewConfig(legacyscheme.Codecs)
genericConfig.MergedResourceConfig = controlplane.DefaultAPIResourceConfigSource()

// 每个 Options 都有 ApplyTo，将命令行参数写入 genericConfig
if lastErr = s.SecureServing.ApplyTo(&genericConfig.SecureServing, &genericConfig.LoopbackClientConfig); lastErr != nil {
    return
}
if lastErr = s.Features.ApplyTo(genericConfig); lastErr != nil { return }
if lastErr = s.EgressSelector.ApplyTo(genericConfig); lastErr != nil { return }
...
```

**ApplyTo 模式**：apiserver 将所有配置选项按功能分组为 `XxxOptions` 结构体（`SecureServingOptions`、`EtcdOptions` 等），每个都有 `AddFlags`（注册命令行参数）和 `ApplyTo`（把解析后的值写进运行时 Config）两个方法。这样配置的来源（命令行 / 默认值）与配置的使用（运行时 struct）完全解耦。

### etcd 存储工厂

```go
// server.go:484
storageFactoryConfig := kubeapiserver.NewStorageFactoryConfig()
storageFactoryConfig.APIResourceConfig = genericConfig.MergedResourceConfig
completedStorageFactoryConfig, _ := storageFactoryConfig.Complete(s.Etcd)
storageFactory, _ = completedStorageFactoryConfig.New()

// 将 storageFactory 注入到 genericConfig，后续通过 RESTOptionsGetter 获取 etcd 句柄
s.Etcd.ApplyWithStorageFactoryTo(storageFactory, genericConfig)
// 效果：genericConfig.RESTOptionsGetter = StorageFactoryRestOptionsFactory{Factory: storageFactory}
```

`RESTOptionsGetter` 是一个接口，后续所有 API group 在注册时通过它获取自己的 etcd 存储后端，而不需要直接持有 etcd 连接。存储工厂还会注册 `addEtcdHealthEndpoint`，在 `/healthz` 端点暴露 etcd 连通性检查。

### loopback 内部通信优化

```go
// server.go:506
genericConfig.LoopbackClientConfig.ContentConfig.ContentType = "application/vnd.kubernetes.protobuf"
genericConfig.LoopbackClientConfig.DisableCompression = true
```

apiserver 内部组件之间调用走 loopback，有两个优化：
- **protobuf** 替代 JSON：内部网络延迟极低，序列化/反序列化 CPU 成本更值得优化，protobuf 比 JSON 体积小、解析快
- **禁用压缩**：内部网络带宽充足，压缩/解压缩反而消耗 CPU，划不来

### clientset 与 versionedInformers

```go
// server.go:511
kubeClientConfig := genericConfig.LoopbackClientConfig
clientgoExternalClient, _ := clientgoclientset.NewForConfig(kubeClientConfig)
versionedInformers = clientgoinformers.NewSharedInformerFactory(clientgoExternalClient, 10*time.Minute)
```

`versionedInformers` 是基于 client-go 的 **SharedInformerFactory**，用于 listAndWatch k8s 对象（Pod、Node 等）。它被传入 Authentication、Authorization 等模块，让这些模块可以从本地缓存读取资源状态，而不是每次都直接查询 etcd，大幅降低热点读压力。


## §03 API核心服务的Authentication认证

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| ApplyTo 触发认证初始化 | [authentication.go](kubernetes/pkg/kubeapiserver/options/authentication.go) | `ApplyTo:441` |
| 认证器 New() 工厂 | [config.go](kubernetes/pkg/kubeapiserver/authenticator/config.go) | `New:95` |
| Token 和 Request 接口定义 | [interfaces.go](kubernetes/staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go) | `Token:28`, `Request:34` |
| union 请求认证链 | [union.go](kubernetes/staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go) | `AuthenticateRequest:53` |

本节从 `buildGenericConfig` 中的 `s.Authentication.ApplyTo(...)` 出发，深入到 `authenticatorConfig.New()` 如何组装认证链。

### 入口：ApplyTo → authenticatorConfig.New()

```go
// pkg/kubeapiserver/options/authentication.go:490
authInfo.Authenticator, openAPIConfig.SecurityDefinitions, err = authenticatorConfig.New()
```

`ApplyTo` 把命令行参数转化为 `Config` 结构体，再调用 `Config.New()` 创建最终的认证器，结果赋给 `genericConfig.Authentication.Authenticator`。

### 两个核心接口

```go
// staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go:28
type Token interface {
    // 针对 Bearer Token：从 token 字符串验证身份
    AuthenticateToken(ctx context.Context, token string) (*Response, bool, error)
}

// interfaces.go:34
type Request interface {
    // 针对 HTTP Request：从请求中提取并验证身份（客户端证书、Header 等）
    AuthenticateRequest(req *http.Request) (*Response, bool, error)
}
```

两个接口分别对应两类信息来源：Token 接口处理从 Authorization Header 中提取的 token 字符串；Request 接口处理需要读取整个 HTTP 请求的认证方式（x509 从 TLS 连接拿证书，Header 认证从 HTTP Header 拿用户信息）。

### New()：逐类收集，两次 union

```go
// pkg/kubeapiserver/authenticator/config.go:95
func (config Config) New() (authenticator.Request, ...) {
    var authenticators []authenticator.Request   // 处理 http.Request 的认证器列表
    var tokenAuthenticators []authenticator.Token // 处理 token 字符串的认证器列表

    // ① 按配置，逐类加入 Request 认证器
    if config.ClientCAContentProvider != nil {
        certAuth := x509.NewDynamic(...)         // x509 客户端证书
        authenticators = append(authenticators, certAuth)
    }
    if config.RequestHeaderConfig != nil {
        authenticators = append(authenticators, requestHeaderAuthenticator) // 代理 Header
    }

    // ② 按配置，逐类加入 Token 认证器
    if len(config.TokenAuthFile) > 0 {
        tokenAuthenticators = append(tokenAuthenticators, tokenFileAuth)
    }
    if config.ServiceAccountIssuer != "" {
        tokenAuthenticators = append(tokenAuthenticators, serviceAccountAuth) // JWT SA Token
    }
    if config.BootstrapToken { ... }            // Bootstrap Token
    if len(config.OIDCIssuerURL) > 0 { ... }   // OIDC JWT

    // ③ 把所有 Token 认证器 union 成一个，再包装成 Request 认证器
    if len(tokenAuthenticators) > 0 {
        tokenAuth := tokenunion.New(tokenAuthenticators...)
        authenticators = append(authenticators, bearertoken.New(tokenAuth)) // 从 Header 提取 token
    }

    // ④ 把所有 Request 认证器 union 成最终认证链
    authenticator := union.New(authenticators...)
    return authenticator, &securityDefinitions, nil
}
```

设计思路：先把功能相似的 Token 认证器收集成一个 token union，再把 token union 和其他 Request 认证器合并成最终的 request union。层次清晰，新增认证方式只需 `append` 一行。

### union 认证链的规则

```go
// staging/src/k8s.io/apiserver/pkg/authentication/request/union/union.go:53
func (authHandler *unionAuthRequestHandler) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    var errlist []error
    for _, currAuthRequestHandler := range authHandler.Handlers {
        resp, ok, err := currAuthRequestHandler.AuthenticateRequest(req)
        if err != nil {
            errlist = append(errlist, err)
            continue       // 某个认证器报错 → 记录错误，继续尝试下一个
        }
        if ok {
            return resp, ok, err  // 某个认证器成功 → 立即返回，短路
        }
    }
    return nil, false, utilerrors.NewAggregate(errlist) // 全部失败 → 聚合所有错误
}
```

三条规则：
1. 某个认证器返回 **error** → 说明认证失败（如证书格式错误），记录错误并继续下一个
2. 某个认证器返回 **ok=true** → 认证通过，立即 return，不再尝试后续认证器（短路）
3. 全部认证器都返回 **ok=false** → 聚合所有错误后返回，最终认证失败



## §04 API核心服务的Authorization鉴权

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| BuildAuthorizer 入口 | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `BuildAuthorizer:568` |
| 鉴权器 New() 工厂 | [config.go](kubernetes/pkg/kubeapiserver/authorizer/config.go) | `New:71` |
| Authorizer / RuleResolver 接口 | [interfaces.go](kubernetes/staging/src/k8s.io/apiserver/pkg/authorization/authorizer/interfaces.go) | `Authorizer:70`, `RuleResolver:81` |
| union 鉴权链 | [union.go](kubernetes/staging/src/k8s.io/apiserver/pkg/authorization/union/union.go) | `Authorize:45` |

本节从 `buildGenericConfig` 中的 `BuildAuthorizer(s, ...)` 出发，深入到 `authorizationConfig.New()` 如何按模式组装鉴权链。

### Authorization 的目的

鉴权（Authorization）解决的是"**你有没有权利做这件事**"，拒绝时返回 HTTP 403。生产环境通常同时启用 Node + RBAC 两种模式：Node 模式专门约束 kubelet 的最小权限（详见 §05），RBAC 负责其他所有主体（详见 §06）。

### 两个核心接口

```go
// staging/src/k8s.io/apiserver/pkg/authorization/authorizer/interfaces.go:70
type Authorizer interface {
    Authorize(ctx context.Context, a Attributes) (authorized Decision, reason string, err error)
}
```

`Decision` 有三种结果：
- `DecisionAllow`：通过
- `DecisionDeny`：拒绝（明确拒绝，直接返回 403）
- `DecisionNoOpinion`：未表态（交给下一个鉴权器判断）

```go
// interfaces.go:81
type RuleResolver interface {
    RulesFor(user user.Info, namespace string) ([]ResourceRuleInfo, []NonResourceRuleInfo, bool, error)
}
```

`RuleResolver` 用于查询某个用户在某命名空间下有哪些规则，供 `kubectl auth can-i` 等场景使用。返回两类规则：`ResourceRuleInfo`（针对 Pod/Service 等资源）和 `NonResourceRuleInfo`（针对 `/metrics` `/healthz` 等非资源 URL）。

### New()：按模式逐个构造，统一 union

```go
// pkg/kubeapiserver/authorizer/config.go:71
func (config Config) New() (authorizer.Authorizer, authorizer.RuleResolver, error) {
    var authorizers   []authorizer.Authorizer
    var ruleResolvers []authorizer.RuleResolver

    for _, authorizationMode := range config.AuthorizationModes {
        switch authorizationMode {
        case modes.ModeNode:
            graph := node.NewGraph()
            node.AddGraphEventHandlers(graph,
                config.VersionedInformerFactory.Core().V1().Nodes(),
                config.VersionedInformerFactory.Core().V1().Pods(), ...)
            nodeAuthorizer := node.NewAuthorizer(graph, ...)
            authorizers    = append(authorizers, nodeAuthorizer)
            ruleResolvers  = append(ruleResolvers, nodeAuthorizer)

        case modes.ModeRBAC:
            rbacAuthorizer := rbac.New(
                &rbac.RoleGetter{Lister: config.VersionedInformerFactory.Rbac().V1().Roles().Lister()},
                &rbac.RoleBindingLister{...},
                &rbac.ClusterRoleGetter{...},
                &rbac.ClusterRoleBindingLister{...},
            )
            authorizers   = append(authorizers, rbacAuthorizer)
            ruleResolvers = append(ruleResolvers, rbacAuthorizer)
        // 同理处理 ABAC / Webhook / AlwaysAllow / AlwaysDeny
        }
    }

    return union.New(authorizers...), union.NewRuleResolvers(ruleResolvers...), nil
}
```

设计与 Authentication 完全对称：按配置的模式顺序逐一构造，最终用 union 串联。模式可以叠加，比如同时启用 Node + RBAC，这是生产环境的标准配置。

Node 鉴权器额外订阅了 Node/Pod/PV/VolumeAttachment 的 Informer 事件，维护一个内存 graph，用于快速查找 kubelet 与 Pod/Volume 的关联关系（详见 §05）。

### union 鉴权链：DecisionNoOpinion 才继续

```go
// staging/src/k8s.io/apiserver/pkg/authorization/union/union.go:45
func (authzHandler unionAuthzHandler) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
    for _, currAuthzHandler := range authzHandler {
        decision, reason, err := currAuthzHandler.Authorize(ctx, a)
        switch decision {
        case authorizer.DecisionAllow, authorizer.DecisionDeny:
            return decision, reason, err  // Allow 或 Deny 均直接返回，短路
        case authorizer.DecisionNoOpinion:
            // 未表态，继续下一个鉴权器
        }
    }
    return authorizer.DecisionNoOpinion, strings.Join(reasonlist, "\n"), ...
}
```

与 Authentication 的 union 逻辑有一处关键区别：Authorization 的 union 对 **error 不短路**（继续下一个），只有 Allow 或 Deny 才短路返回。这是因为鉴权器之间是独立的，一个鉴权器内部错误（如 RBAC 读取失败）不应阻断 Node 鉴权器的判断。


## §05 node类型的Authorization鉴权

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Node 鉴权主入口 | [node_authorizer.go](kubernetes/plugin/pkg/auth/authorizer/node/node_authorizer.go) | `Authorize:94` |
| 读命名空间对象（secret/configmap） | [node_authorizer.go](kubernetes/plugin/pkg/auth/authorizer/node/node_authorizer.go) | `authorizeReadNamespacedObject:175` |
| 只读 get（pv/pvc/va） | [node_authorizer.go](kubernetes/plugin/pkg/auth/authorizer/node/node_authorizer.go) | `authorizeGet:161` |
| 图路径查找（节点-资源关系） | [node_authorizer.go](kubernetes/plugin/pkg/auth/authorizer/node/node_authorizer.go) | `authorize:195` |
| 静态 Node 规则定义 | [policy.go](kubernetes/plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go) | `NodeRules:106` |
| VerbMatches / NonResourceURLMatches | [evaluation_helpers.go](kubernetes/pkg/apis/rbac/v1/evaluation_helpers.go) | `VerbMatches:31`, `NonResourceURLMatches:99` |

本节深入 Node 鉴权器的四条核心规则及其底层实现——以 `graph` 建立节点与资源的关系，通过路径查找判断 kubelet 是否有权访问特定资源。

### 节点鉴权的设计目标

Node 鉴权是专门针对 kubelet 的最小权限鉴权器。kubelet 需要读写 Pod、Node、Secret、ConfigMap 等资源，但**只应访问与自身节点相关的资源**，不能跨节点获取其他 kubelet 的 Secret。Node 鉴权器通过维护一个资源关系图来强制这一约束。

kubelet 的典型操作权限：
- **读取**：`services`、`endpoints`、`nodes`、`pods`、以及调度到本节点的 Pod 所引用的 `secrets`、`configmaps`、`pvcs`、`pvs`
- **写入**：节点状态、Pod 状态、事件（只能修改自身节点/Pod）
- **鉴权相关**：`tokenreviews`、`subjectaccessreviews`（TLS Bootstrap 流程需要）

### Authorize：四条规则

```go
// plugin/pkg/auth/authorizer/node/node_authorizer.go:94
func (r *NodeAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    nodeName, isNode := r.identifier.NodeIdentity(attrs.GetUser())

    // 规则1：请求方不是 node → DecisionNoOpinion（未表态，相当于拒绝）
    if !isNode {
        return authorizer.DecisionNoOpinion, "", nil
    }
    // 规则2：nodeName 找不到（无法识别身份）→ DecisionNoOpinion
    if len(nodeName) == 0 {
        return authorizer.DecisionNoOpinion, "unknown node for user ...", nil
    }

    if attrs.IsResourceRequest() {
        requestResource := schema.GroupResource{Group: attrs.GetAPIGroup(), Resource: attrs.GetResource()}
        switch requestResource {
        // 规则3：secret / configmap / pvc / pv 等敏感资源 → 细粒度校验
        case secretResource:
            return r.authorizeReadNamespacedObject(nodeName, secretVertexType, attrs)
        case configMapResource:
            return r.authorizeReadNamespacedObject(nodeName, configMapVertexType, attrs)
        case pvcResource:
            return r.authorizeGet(nodeName, pvcVertexType, attrs)
        case pvResource:
            return r.authorizeGet(nodeName, pvVertexType, attrs)
        // ... 其他资源类型
        }
    }

    // 规则4：其他资源 → 对比静态 NodeRules 规则表
    if rbac.RulesAllow(attrs, r.nodeRules...) {
        return authorizer.DecisionAllow, "", nil
    }
    return authorizer.DecisionNoOpinion, "", nil
}
```

### 规则3：authorizeReadNamespacedObject

```go
// node_authorizer.go:175
func (r *NodeAuthorizer) authorizeReadNamespacedObject(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    switch attrs.GetVerb() {
    case "get", "list", "watch": // 只允许读操作
    default:
        return authorizer.DecisionNoOpinion, "can only read resources of this type", nil
    }
    if len(attrs.GetSubresource()) > 0 {
        return authorizer.DecisionNoOpinion, "cannot read subresource", nil
    }
    if len(attrs.GetNamespace()) == 0 {
        return authorizer.DecisionNoOpinion, "can only read namespaced object of this type", nil
    }
    return r.authorize(nodeName, startingType, attrs) // 最终校验图路径
}
```

三层前置拦截：动词必须是只读（非 get/list/watch 直接拒绝）→ 不允许访问子资源 → 必须有命名空间。通过后进入图路径查找。

### 底层 authorize：图路径查找

```go
// node_authorizer.go:195
func (r *NodeAuthorizer) authorize(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    if len(attrs.GetName()) == 0 {
        return authorizer.DecisionNoOpinion, "No Object name found", nil
    }
    ok, err := r.hasPathFrom(nodeName, startingType, attrs.GetNamespace(), attrs.GetName())
    if !ok || err != nil {
        return authorizer.DecisionNoOpinion, "no relationship found between node and this object", nil
    }
    return authorizer.DecisionAllow, "", nil
}
```

`hasPathFrom` 在内存 graph 中查询：从目标资源（如 secret）出发，是否存在一条路径连接到请求方 kubelet 所在的节点。graph 由 §04 中构造 Node 鉴权器时挂载的 Informer（Node/Pod/PV/VolumeAttachment）实时维护，本质是 **Pod 调度信息的拓扑图**：Pod 引用 Secret → Pod 调度到 Node → kubelet 有权读该 Secret。

### 规则4：静态 NodeRules

对于 secret/configmap/pvc 等之外的其他资源（如 endpoints、tokenreviews），Node 鉴权器不走图路径查找，而是直接用 `RulesAllow` 对比 `NodeRules()` 中预定义的静态规则表：

```go
// plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go:106
func NodeRules() []rbacv1.PolicyRule {
    // 允许 kubelet 读取 services, endpoints
    // 允许 kubelet 创建 tokenreviews, subjectaccessreviews（TLS Bootstrap 用）
    // 允许 kubelet 读/写自身 node 和 pod 状态
    // 允许 kubelet 创建/更新 events
    ...
}
```

`VerbMatches` 支持通配符 `*`（匹配任意动词），`NonResourceURLMatches` 支持前缀匹配（用于 `/metrics` 等路径）。



## §06 rbac类型的Authorization鉴权

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| RBACAuthorizer.Authorize | [rbac.go](kubernetes/plugin/pkg/auth/authorizer/rbac/rbac.go) | `Authorize:75` |
| visit 回调 + RuleAllows | [rbac.go](kubernetes/plugin/pkg/auth/authorizer/rbac/rbac.go) | `visit:63`, `RuleAllows:178` |
| VisitRulesFor 遍历绑定 | [rule.go](kubernetes/pkg/registry/rbac/validation/rule.go) | `VisitRulesFor:179` |
| appliesTo / appliesToUser | [rule.go](kubernetes/pkg/registry/rbac/validation/rule.go) | `appliesTo:263`, `appliesToUser:281` |

本节深入 RBAC 鉴权的完整查找链：`Authorize` → `VisitRulesFor`（ClusterRoleBinding → RoleBinding）→ `appliesTo`（Subject 匹配）→ `GetRoleReferenceRules`（获取 rules）→ `visit` 回调（RuleAllows 逐条比对）。

### RBACAuthorizer.Authorize：Visitor 模式

```go
// plugin/pkg/auth/authorizer/rbac/rbac.go:75
func (r *RBACAuthorizer) Authorize(ctx context.Context, requestAttributes authorizer.Attributes) (authorizer.Decision, string, error) {
    ruleCheckingVisitor := &authorizingVisitor{requestAttributes: requestAttributes}

    // 遍历所有绑定，对每条 rule 调用 visitor.visit
    r.authorizationRuleResolver.VisitRulesFor(
        requestAttributes.GetUser(),
        requestAttributes.GetNamespace(),
        ruleCheckingVisitor.visit,
    )

    if ruleCheckingVisitor.allowed {
        return authorizer.DecisionAllow, ruleCheckingVisitor.reason, nil
    }
    return authorizer.DecisionNoOpinion, reason, nil
}
```

`authorizingVisitor` 是一个带状态的 Visitor：持有 `allowed bool` 标志，一旦某条规则匹配就置为 true 并停止遍历。这是 Visitor 模式在鉴权中的典型应用——把"遍历"和"判断"解耦，`VisitRulesFor` 只负责喂数据，`visit` 只负责判断。

```go
// rbac.go:63
func (v *authorizingVisitor) visit(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool {
    if rule != nil && RuleAllows(v.requestAttributes, rule) {
        v.allowed = true
        v.reason = fmt.Sprintf("RBAC: allowed by %s", source.String())
        return false // 返回 false → 停止继续遍历（短路）
    }
    if err != nil { v.errors = append(v.errors, err) }
    return true // 继续遍历下一条规则
}
```

### VisitRulesFor：两阶段遍历

```go
// pkg/registry/rbac/validation/rule.go:179
func (r *DefaultRuleResolver) VisitRulesFor(user user.Info, namespace string,
    visitor func(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool) {

    // 阶段1：遍历所有 ClusterRoleBinding（集群级，不限命名空间）
    clusterRoleBindings, _ := r.clusterRoleBindingLister.ListClusterRoleBindings()
    for _, crb := range clusterRoleBindings {
        subjectIndex, applies := appliesTo(user, crb.Subjects, "")
        if !applies { continue }
        rules, _ := r.GetRoleReferenceRules(crb.RoleRef, "")  // 从 ClusterRole 取 rules
        for i := range rules {
            if !visitor(sourceDescriber, &rules[i], nil) { return } // visit 返回 false → 停止
        }
    }

    // 阶段2：遍历命名空间内的 RoleBinding（仅当有 namespace 时）
    if len(namespace) > 0 {
        roleBindings, _ := r.roleBindingLister.ListRoleBindings(namespace)
        for _, rb := range roleBindings {
            subjectIndex, applies := appliesTo(user, rb.Subjects, namespace)
            if !applies { continue }
            rules, _ := r.GetRoleReferenceRules(rb.RoleRef, namespace) // 从 Role/ClusterRole 取 rules
            for i := range rules {
                if !visitor(sourceDescriber, &rules[i], nil) { return }
            }
        }
    }
}
```

两个阶段顺序固定：先查 ClusterRoleBinding，再查 RoleBinding。`GetRoleReferenceRules` 通过 Informer 本地缓存（`roleGetter` / `clusterRoleGetter`）获取 rules，不走 etcd，性能很高。

### appliesTo：Subject 匹配

```go
// rule.go:281
func appliesToUser(user user.Info, subject rbacv1.Subject, namespace string) bool {
    switch subject.Kind {
    case rbacv1.UserKind:
        return user.GetName() == subject.Name             // 普通用户：比较用户名
    case rbacv1.GroupKind:
        return has(user.GetGroups(), subject.Name)        // 组：检查用户是否属于该组
    case rbacv1.ServiceAccountKind:
        saNamespace := namespace
        if len(subject.Namespace) > 0 { saNamespace = subject.Namespace }
        // ServiceAccount 全名格式：system:serviceaccount:<namespace>:<name>
        return serviceaccount.MatchesUsername(saNamespace, subject.Name, user.GetName())
    }
    return false
}
```

### RuleAllows：规则比对

```go
// rbac.go:178
func RuleAllows(requestAttributes authorizer.Attributes, rule *rbacv1.PolicyRule) bool {
    if requestAttributes.IsResourceRequest() {
        combinedResource := requestAttributes.GetResource()
        if len(requestAttributes.GetSubresource()) > 0 {
            combinedResource = requestAttributes.GetResource() + "/" + requestAttributes.GetSubresource()
        }
        return VerbMatches(rule, requestAttributes.GetVerb()) &&      // 动词匹配
            APIGroupMatches(rule, requestAttributes.GetAPIGroup()) &&  // apiGroup 匹配
            ResourceMatches(rule, combinedResource, ...) &&           // 资源匹配
            ResourceNameMatches(rule, requestAttributes.GetName())    // 资源名匹配
    }
    // 非资源请求（/metrics、/healthz 等）
    return VerbMatches(rule, ...) && NonResourceURLMatches(rule, requestAttributes.GetPath())
}
```

所有 `*Matches` 函数都支持通配符 `*`（VerbAll / NonResourceAll），实现"全部放行"。`NonResourceURLMatches` 额外支持前缀通配（`/api/*`）。


## §07 audit审计功能说明和源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| AuditOptions.ApplyTo（5步构建） | [audit.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/options/audit.go) | `ApplyTo:281` |
| Sink / Backend 接口定义 | [types.go](kubernetes/staging/src/k8s.io/apiserver/pkg/audit/types.go) | `Sink:23`, `Backend:32` |
| HTTP 层注入审计 | [audit.go](kubernetes/staging/src/k8s.io/apiserver/pkg/endpoints/filters/audit.go) | `WithAudit:42` |

本节从 `buildGenericConfig` 中的 `s.AuditOptions.ApplyTo(genericConfig)` 出发，走到 Backend 的 5 步构建，再到 HTTP Handler 层的审计事件写入。

### ApplyTo：5步构建 Backend

```go
// staging/src/k8s.io/apiserver/pkg/server/options/audit.go:281
func (o *AuditOptions) ApplyTo(c *server.Config) error {

    // 1. 从 --audit-policy-file 加载审计策略
    checker, err := o.newPolicyChecker()

    // 2. 从 --audit-log-path 构建 logBackend
    //    "-" 代表写入 stdout；否则写入指定文件（lumberjack 滚动日志）
    var logBackend audit.Backend
    if w := o.LogOptions.getWriter(); w != nil {
        logBackend = o.LogOptions.newBackend(w)
    }

    // 3. 根据配置构建 webhookBackend（HTTP 回调审计）
    var webhookBackend audit.Backend
    if o.WebhookOptions.enabled() {
        webhookBackend, err = o.WebhookOptions.newUntruncatedBackend(egressDialer)
    }

    // 4. 如果有 webhook，封装为 dynamicBackend（加 truncate 截断选项）
    var dynamicBackend audit.Backend
    if webhookBackend != nil {
        dynamicBackend = o.WebhookOptions.TruncateOptions.wrapBackend(webhookBackend, groupVersion)
    }

    // 5. 设置审计策略计算对象 + 合并 Backend
    c.AuditPolicyChecker = checker
    c.AuditBackend = appendBackend(logBackend, dynamicBackend) // union 合并
}
```

`appendBackend` 与鉴权/认证的 `union` 逻辑相同：把 logBackend 和 dynamicBackend 合并为一个，审计事件会同时投递给两者。

### Sink 与 Backend 接口

```go
// staging/src/k8s.io/apiserver/pkg/audit/types.go:23
type Sink interface {
    // ProcessEvents 投递审计事件；同一个 auditID 可能被调用最多三次（请求收到、响应开始、响应完成）
    // 必须对 event 做 deepcopy（调用方会复用这个对象）
    ProcessEvents(events ...*auditinternal.Event) bool
}

type Backend interface {
    Sink
    Run(stopCh <-chan struct{}) error  // 非阻塞启动，可在 goroutine 中投递
    Shutdown()                         // 优雅关闭，确保未投递的事件全部送达
    String() string
}
```

`Backend` 在 `Sink` 的基础上增加生命周期管理（Run/Shutdown），与 stopCh 信号对齐，保证进程退出前所有审计事件都已写出。

logBackend 的 `ProcessEvents` 调用 `fmt.Fprint(b.out, line)` 直接写文件；格式支持 Legacy 和 Json 两种，通过 `--audit-log-format` 控制。

### HTTP 层注入：WithAudit

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/audit.go:42
func WithAudit(handler http.Handler, sink audit.Sink, policy policy.Checker,
    longRunningCheck request.LongRunningRequestCheck) http.Handler {
    // 如果 sink 和 policy 都为 nil，不加装饰，直接透传
    if sink == nil || policy == nil {
        return handler
    }
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        req, ev, omitStages, err := createAuditEventAndAttachToContext(req, policy)
        // ...
        ev.Stage = auditinternal.StageRequestReceived
        processAuditEvent(ctx, sink, ev, omitStages) // 第1次：请求收到

        // 处理请求
        handler.ServeHTTP(w, req)

        // 第2次/第3次：响应开始 / 响应完成（在 ResponseWriter 包装器中触发）
    })
}
```

审计作为 HTTP 中间件层插入，对业务 handler 完全透明。每个请求最多触发三次 `ProcessEvents`：请求收到（StageRequestReceived）→ 响应头发出（StageResponseStarted）→ 响应完成（StageResponseComplete）。`policy.Checker` 决定当前请求应记录哪个级别，`omitStages` 记录哪些阶段不需要记录。



## §08 admission准入控制器功能和源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| admissionConfig.New（初始化 PluginInitializer） | [config.go](kubernetes/pkg/kubeapiserver/admission/config.go) | `New:48` |
| s.Admission.ApplyTo（开关计算 + 链组装） | [server.go](kubernetes/cmd/kube-apiserver/app/server.go) | `buildGenericConfig` 内 |
| NewFromPlugins（遍历实例化插件链） | [plugins.go](kubernetes/staging/src/k8s.io/apiserver/pkg/admission/plugins.go) | `NewFromPlugins:128` |
| InitPlugin（工厂取插件 + Initialize） | [plugins.go](kubernetes/staging/src/k8s.io/apiserver/pkg/admission/plugins.go) | `InitPlugin:167` |
| RegisterAllAdmissionPlugins（统一注册） | [plugins.go](kubernetes/pkg/kubeapiserver/options/plugins.go) | `RegisterAllAdmissionPlugins:107` |
| AlwaysPullImages 示例（Admit + Validate） | [admission.go](kubernetes/plugin/pkg/admission/alwayspullimages/admission.go) | `Admit:61`, `Validate:80` |

本节从 `admissionConfig.New` 出发，走到插件注册（Factory）→ 插件实例化（NewFromPlugins/InitPlugin）→ Admit/Validate 两阶段执行。

### 准入控制器的位置与作用

```
API request
  │
  ▼ authentication / authorization（身份 + 权限）
  │
  ▼ mutating admission controllers    ← 第一阶段：可修改对象
  │   （如 AlwaysPullImages：把 imagePullPolicy 改成 Always）
  │
  ▼ object schema validation
  │
  ▼ validating admission controllers  ← 第二阶段：只验证，不修改
  │   （如 AlwaysPullImages.Validate：校验 imagePullPolicy 确实是 Always）
  │
  ▼ persisted to etcd
```

准入控制器在认证/授权之后、持久化之前拦截请求。任一控制器拒绝 → 整个请求立即被拒绝。实现 `MutationInterface` 的插件参与 Mutating 阶段，实现 `ValidationInterface` 的参与 Validating 阶段，两阶段严格隔离（见本节末尾 AlwaysPullImages 示例）。

### 初始化入口：admissionConfig.New

```go
// pkg/kubeapiserver/admission/config.go:48
func (c *Config) New(proxyTransport *http.Transport, ...) ([]admission.PluginInitializer, ...) {
    // 构建 kubePluginInitializer：持有 cloudConfig, RESTMapper, quotaConfiguration
    // Initialize(plugin) 时，检查插件是否实现对应 Wants 接口，若是则注入依赖
    kubePluginInitializer := NewPluginInitializer(cloudConfig, discoveryClient, quotaConfig)

    // admissionPostStartHook：每 30s 重置 discoveryRESTMapper（动态发现 API 资源）
    admissionPostStartHook = func(ctx genericapiserver.PostStartHookContext) error {
        go utilwait.Until(discoveryRESTMapper.Reset, 30*time.Second, ctx.StopCh)
        return nil
    }

    return []admission.PluginInitializer{webhookPluginInitializer, kubePluginInitializer}, admissionPostStartHook, nil
}
```

`PluginInitializer` 的 `Initialize(plugin)` 用接口探测（类型断言）的方式注入依赖——插件声明自己需要什么（`WantsRESTMapper`、`WantsCloudConfig` 等），初始化器按需注入，解耦了插件与框架。

### 插件开关计算：computePluginNames

`s.Admission.ApplyTo` 接收三个来源计算最终插件列表：
- `--admission-control` 命令行传入的 `PluginNames`
- `RecommendedPluginOrder`（官方推荐的完整有序列表 `AllOrderedPlugins`）
- `--disable-admission-plugins` 显式关闭的插件

`computePluginNames` 计算出 `orderedPlugins`（保持推荐顺序，仅保留开启的），再读取 `--admission-control-config-file`，构造 `genericInitializer` 和 `initializersChain`，最终调用 `NewFromPlugins`。

### 插件实例化：NewFromPlugins + InitPlugin

```go
// staging/src/k8s.io/apiserver/pkg/admission/plugins.go:128
func (ps *Plugins) NewFromPlugins(pluginNames []string, ...) (Interface, error) {
    var handlers []Interface
    for _, pluginName := range pluginNames {
        pluginConfig, _ := configProvider.ConfigFor(pluginName)
        plugin, _ := ps.InitPlugin(pluginName, pluginConfig, pluginInitializer)
        handlers = append(handlers, plugin)
        // 统计 MutationInterface / ValidationInterface 以便打印日志
    }
    return newReinvocationHandler(chainAdmissionHandler(handlers)), nil
}

// plugins.go:167
func (ps *Plugins) InitPlugin(name string, config io.Reader, pluginInitializer PluginInitializer) (Interface, error) {
    plugin, found, err := ps.getPlugin(name, config)  // 从 registry map 取工厂函数并调用
    pluginInitializer.Initialize(plugin)               // 注入 RESTMapper、clientset 等依赖
    ValidateInitialization(plugin)                     // 校验插件初始化完整性
    return plugin, nil
}
```

`registry` 是 `map[string]Factory`，`Factory` 的类型是 `func(config io.Reader) (Interface, error)`。所有插件在 `RegisterAllAdmissionPlugins` 中统一调用 `plugins.Register(PluginName, Factory)` 注册。

### 插件注册与实现：以 AlwaysPullImages 为例

```go
// plugin/pkg/admission/alwayspullimages/admission.go:45
func Register(plugins *admission.Plugins) {
    plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
        return NewAlwaysPullImages(), nil  // 工厂函数：返回插件实例
    })
}

// Admit（Mutating 阶段）：把所有容器的 imagePullPolicy 改成 Always
func (a *AlwaysPullImages) Admit(ctx context.Context, attributes admission.Attributes, ...) error {
    pod, _ := attributes.GetObject().(*api.Pod)
    pods.VisitContainersWithPath(&pod.Spec, field.NewPath("spec"), func(c *api.Container, _ *field.Path) bool {
        c.ImagePullPolicy = api.PullAlways  // 直接修改对象（Mutating）
        return true
    })
    return nil
}

// Validate（Validating 阶段）：校验 imagePullPolicy 确实是 Always
func (*AlwaysPullImages) Validate(ctx context.Context, attributes admission.Attributes, ...) error {
    pod, _ := attributes.GetObject().(*api.Pod)
    pods.VisitContainersWithPath(&pod.Spec, ..., func(c *api.Container, p *field.Path) bool {
        if c.ImagePullPolicy != api.PullAlways {
            allErrs = append(allErrs, admission.NewForbidden(...))
        }
        return true
    })
    return utilerrors.NewAggregate(allErrs)
}
```

Admit 和 Validate 分两阶段执行，解耦了"修改"和"校验"的职责。即使 Admit 已经改了，Validate 阶段仍会再验证一次，确保链中其他 Mutating 插件没有覆盖掉这里的修改。
