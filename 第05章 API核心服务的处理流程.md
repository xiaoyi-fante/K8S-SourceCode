# 第05章 API 核心服务的处理流程

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 05 章 — API 核心服务的处理流程
> **源码入口**: `pkg/controlplane/instance.go`

---

## 核心机制一览

1. **`GenericAPIServer.New` 是三个 server 共用的构造模板**：aggregatorServer、kubeAPIServer、apiExtensionsServer 都通过 `completedConfig.New(name, delegationTarget)` 创建各自的 `GenericAPIServer`，name 区分日志，delegationTarget 串联委托链，handler chain 由 `BuildHandlerChainFunc` 可插拔注入。

2. **PostStartHook 是 server 启动后的扩展点**：`New` 在构造时将 `delegationTarget`、`completedConfig.PostStartHooks`、内置 informer hook 三路来源合并，server `Run()` 后统一触发；admission `discoveryRESTMapper.Reset` 就是通过此机制注入的。

3. **`installAPI` 注册四类非业务路由**：`/`（路由索引）、`/debug/pprof`（性能分析）、`/metrics`（指标监控）、`/version`（版本信息）和 `/apis`（发现接口），这些与业务 API 路由分开注册。

4. **Scheme 是 GVK ↔ Go Type 的双向索引**：以四张 `map` 实现 O(1) 查找：`gvkToType`（GVK→Type）、`typeToGVK`（Type→GVK，一对多）、`unversionedTypes`（无版本 Type→GVK）、`unversionedKinds`（Kind 名→Type）。

5. **RESTStorage 是每个资源的 CRUD 实现载体**：每种资源调用各自的 `NewREST`/`NewStorage` 创建，底层嵌入 `*genericregistry.Store`（含 etcd 读写逻辑）；Pod 额外返回 `PodStorage`，携带 Attach/Exec/Log/PortForward 等子资源的独立 RESTStorage。

6. **`restStorageMap` 是路由注册的最终数据结构**：`NewLegacyRESTStorage` 将所有资源和子资源的 RESTStorage 以 `"pods"` / `"pods/status"` / `"pods/exec"` 为 key 塞入 `map[string]rest.Storage`，再经 `apiGroupInfo.VersionedResourcesStorageMap["v1"]` 传给 `InstallLegacyAPI` 完成路由注册。

7. **`Strategy.PrepareForCreate` 强制初始化资源状态**：Pod 写入 etcd 前，`BeforeCreate` 回调 `podStrategy.PrepareForCreate`，强制将 `Status.Phase` 设为 `Pending`、计算并写入 `QOSClass`——用户在 YAML 中写的 Status 字段被 apiserver 覆盖，防止客户端伪造状态。

8. **etcd 事务是防并发重复创建的最后屏障**：`etcd3.store.Create` 用 `IF version == 0 THEN PUT` 原子事务写入，version == 0 表示 key 不存在；并发创建同名资源时，只有一个能成功，另一个拿到 `KeyExistsError`。

9. **MaxInFlightLimit 与 APF 互斥，由 `c.FlowControl` 决定**：`DefaultBuildHandlerChain` 中，`FlowControl != nil` 走 APF（优先级队列+FlowSchema），否则走 MaxInFlightLimit（两个 bool channel 信号量）；`system:masters` 组的请求在 MaxInFlightLimit 中无条件放行。

---

## 全章调用链总图

```
CreateServerChain
  │
  ├── createAPIExtensionsServer
  │     └── c.GenericConfig.New("apiextensions-apiserver", delegationTarget)
  │
  ├── CreateKubeAPIServer
  │     └── completedConfig.New(delegationTarget)            instance.go
  │           ├── c.GenericConfig.New("kube-apiserver", delegationTarget)
  │           │     └── GenericAPIServer{...}                config.go:538
  │           │           ├── NewAPIServerHandler(name, ...)
  │           │           ├── 合并 PostStartHooks（3路来源）
  │           │           └── installAPI(s, c)              config.go:773
  │           │                 ├── routes.Index
  │           │                 ├── routes.Profiling
  │           │                 ├── routes.Metrics/MetricsWithReset
  │           │                 ├── routes.Version
  │           │                 └── DiscoveryGroupManager.WebService()
  │           │
  │           ├── m := &Instance{GenericAPIServer: s, ...}
  │           │
  │           ├── InstallLegacyAPI（§01 核心服务）           instance.go:541
  │           │     └── legacyRESTStorageProvider.NewLegacyRESTStorage
  │           │           ├── 构造 apiGroupInfo（scheme/codec）
  │           │           ├── 各资源 NewREST/NewStorage → restStorageMap（§02）
  │           │           └── apiGroupInfo.VersionedResourcesStorageMap["v1"] = restStorageMap
  │           │
  │           └── InstallAPIs（扩展 API groups）             instance.go:574
  │
  └── createAggregatorServer
        └── c.GenericConfig.New("kube-aggregator", delegationTarget)
  │
  ▼ server.PrepareRun() → prepared.Run(stopCh)              genericapiserver.go:328
        ├── NonBlockingRun → SecureServingInfo.ServeWithListenerStopped
        └── 触发所有 PostStartHook goroutine
```

---

## §01 API 核心 server 的启动流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| GenericAPIServer 通用构造 | [config.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/config.go) | `completedConfig.New:538` |
| 四类非业务路由注册 | [config.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/config.go) | `installAPI:773` |
| 核心 API server 初始化 | [instance.go](kubernetes/pkg/controlplane/instance.go) | `completedConfig.New:349` |
| 最终启动 Run | [genericapiserver.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go) | `preparedGenericAPIServer.Run:328` |
| HTTPS 监听 | [genericapiserver.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go) | `NonBlockingRun:372` |

本节从 `CreateServerChain` 出发，走到三个 server 共用的 `GenericAPIServer.New`、各自的业务初始化，再到 `PrepareRun().Run(stopCh)` 正式开始监听请求。

### 三个 server 都调用 completedConfig 的 New

```
createAPIExtensionsServer:  genericServer, err := c.GenericConfig.New("apiextensions-apiserver", delegationTarget)
CreateKubeAPIServer:        s, err := c.GenericConfig.New("kube-apiserver", delegationTarget)
createAggregatorServer:     genericServer, err := c.GenericConfig.New("kube-aggregator", delegationTarget)
```

三次调用形态完全相同，name 只用于日志区分，`delegationTarget` 将三个 server 串成委托链。

### completedConfig.New：生成通用 GenericAPIServer

```go
// staging/src/k8s.io/apiserver/pkg/server/config.go:538
func (c completedConfig) New(name string, delegationTarget DelegationTarget) (*GenericAPIServer, error) {
    // HandlerChain：权限校验、审计、限流等中间件在这里注入
    handlerChainBuilder := func(handler http.Handler) http.Handler {
        return c.BuildHandlerChainFunc(handler, c.Config)
    }
    apiServerHandler := NewAPIServerHandler(name, c.Serializer, handlerChainBuilder,
        delegationTarget.UnprotectedHandler())

    s := &GenericAPIServer{
        DiscoveryGroupManager:      discovery.NewRootAPIsHandler(...),
        Handler:                    apiServerHandler,
        delegationTarget:           delegationTarget,
        // ... 其他字段来自 completedConfig
    }
    // ...
}
```

`BuildHandlerChainFunc` 默认实现是 `DefaultBuildHandlerChain`，将 Authentication / Authorization / Audit / Admission 等中间件按顺序包裹在业务 handler 外层。这里是整个权限体系插入 HTTP 路由的接入点。

### 合并 PostStartHook：三路来源

```go
// config.go:612
// 来源1：delegationTarget 的 hooks（链尾向上传递）
for k, v := range delegationTarget.PostStartHooks() {
    s.postStartHooks[k] = v
}
for k, v := range delegationTarget.PreShutdownHooks() {
    s.preShutdownHooks[k] = v
}

// 来源2：completedConfig 中预配置的 hooks（如 admission 的 discoveryRESTMapper.Reset）
for name, preconfiguredPostStartHook := range c.PostStartHooks {
    if err := s.AddPostStartHook(name, preconfiguredPostStartHook.hook); err != nil {
        return nil, err
    }
}

// 来源3：内置 hooks（SharedInformerFactory.Start、priority-and-fairness 等）
if c.SharedInformerFactory != nil {
    err := s.AddPostStartHook("generic-apiserver-start-informers", func(context PostStartHookContext) error {
        c.SharedInformerFactory.Start(context.StopCh)
        return nil
    })
}
```

设计意图：PostStartHook 是 server 启动后的扩展点，三路来源保证各层（委托链下层、配置层、框架层）都能注册启动后行为，同时通过 `DisabledPostStartHooks` 允许选择性关闭。

### installAPI：四类非业务路由注册

```go
// config.go:773
func installAPI(s *GenericAPIServer, c *Config) {
    if c.EnableIndex {
        routes.Index{}.Install(s.listedPathProvider, s.Handler.NonGoRestfulMux)  // /
    }
    if c.EnableProfiling {
        routes.Profiling{}.Install(s.Handler.NonGoRestfulMux)  // /debug/pprof
        // 仅当同时开启 ContentionProfiling 时注册 /debug/pprof/mutex
    }
    if c.EnableMetrics {
        if c.EnableProfiling {
            routes.MetricsWithReset{}.Install(s.Handler.NonGoRestfulMux)  // /metrics（带重置）
        } else {
            routes.DefaultMetrics{}.Install(s.Handler.NonGoRestfulMux)
        }
    }
    routes.Version{Version: c.Version}.Install(s.Handler.GoRestfulContainer)  // /version
    if c.EnableDiscovery {
        s.Handler.GoRestfulContainer.Add(s.DiscoveryGroupManager.WebService())  // /apis（发现接口）
    }
}
```

注意两个 Handler 的区别：`NonGoRestfulMux` 处理原生 `http.Handler`（/metrics、/debug 等）；`GoRestfulContainer` 是 go-restful 框架路由（/version、/apis），支持更丰富的内容协商和 API 文档生成。

### apiserver 核心服务的初始化：Instance.New

```go
// pkg/controlplane/instance.go:349
func (c completedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*Instance, error) {
    // 1. 调用通用 GenericAPIServer.New，得到通用 server
    s, err := c.GenericConfig.New("kube-apiserver", delegationTarget)

    // 2. 用通用 server + 额外配置实例化 Instance（kubeAPIServer 特有结构）
    m := &Instance{
        GenericAPIServer:        s,
        ClusterAuthenticationInfo: c.ExtraConfig.ClusterAuthenticationInfo,
    }

    // 3. 注册核心资源的 REST API（v1 group：Pod、Service、ConfigMap 等）
    if c.ExtraConfig.APIResourceConfigSource.VersionEnabled(apiv1.SchemeGroupVersion) {
        legacyRESTStorageProvider := corerest.LegacyRESTStorageProvider{...}
        if err := m.InstallLegacyAPI(&c, c.GenericConfig.RESTOptionsGetter,
                legacyRESTStorageProvider); err != nil {
            return nil, err
        }
    }

    // 4. 注册扩展 API groups（apps、batch、autoscaling 等）
    if err := m.InstallAPIs(c.ExtraConfig.APIResourceConfigSource,
            c.GenericConfig.RESTOptionsGetter, restStorageProviders...); err != nil {
        return nil, err
    }

    return m, nil
}
```

### 最终的 apiserver 启动流程：Run → NonBlockingRun

```go
// cmd/kube-apiserver/app/server.go（第03章已分析）
func Run(completeOptions completedServerRunOptions, stopCh <-chan struct{}) error {
    server, err := CreateServerChain(completeOptions, stopCh)
    prepared, err := server.PrepareRun()
    return prepared.Run(stopCh)
}

// genericapiserver.go:328
func (s preparedGenericAPIServer) Run(stopCh <-chan struct{}) error {
    stoppedCh, listenerStoppedCh, err := s.NonBlockingRun(stopHttpServerCh, shutdownTimeout)
    // 阻塞直到 stopCh 关闭（SIGTERM）
}

// genericapiserver.go:372
func (s preparedGenericAPIServer) NonBlockingRun(stopCh <-chan struct{}) (<-chan struct{}, error) {
    if s.SecureServingInfo != nil && s.Handler != nil {
        stoppedCh, listenerStoppedCh, err = s.SecureServingInfo.ServeWithListenerStopped(
            s.Handler, shutdownTimeout, stopCh)
    }
    // 触发所有 PostStartHook
    s.RunPostStartHooks(stopCh)
    return stoppedCh, nil
}
```

`ServeWithListenerStopped` 在独立 goroutine 中启动 HTTPS server（`ListenAndServeTLS`），返回两个 channel：`stoppedCh`（所有 active 请求处理完毕后关闭）和 `listenerStoppedCh`（listener 停止接受新连接后关闭），用于优雅停机的两阶段等待。

---

## §02 scheme 和 RESTStorage 的初始化

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Scheme 结构体定义 | [scheme.go](kubernetes/staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go) | `Scheme:46` |
| NewLegacyRESTStorage 总入口 | [storage_core.go](kubernetes/pkg/registry/core/rest/storage_core.go) | `NewLegacyRESTStorage:104` |
| restStorageMap 组装 | [storage_core.go](kubernetes/pkg/registry/core/rest/storage_core.go) | `NewLegacyRESTStorage:269` |
| ConfigMap RESTStorage | [storage.go](kubernetes/pkg/registry/core/configmap/storage/storage.go) | `NewREST:38` |
| Pod RESTStorage | [storage.go](kubernetes/pkg/registry/core/pod/storage/storage.go) | `NewStorage:72` |

本节从 `InstallLegacyAPI` 出发，深入 Scheme 的四张 map 设计，再到 `NewLegacyRESTStorage` 如何组装 `restStorageMap`。

### K8s 资源的 GVK 体系

```
Kubernetes Resources（内置资源）
Custom Resources（自定义资源）
    │
    ├── Group（资源组 / APIGroup）
    │     └── Version（资源版本 / APIVersions）
    │           ├── Resource / SubResource（资源/子资源 / APIResource）
    │           ├── Kind（资源种类，描述 Resource 的种类）
    │           └── Verbs：create / delete / deletecollection / get / list / patch / update / watch
    │
    └── External Version（外部版本，面向用户的 YAML/JSON）
          Internal Version（内部版本，apiserver 内部转换中枢）
```

- **Group**：如 `""` (core)、`apps`、`batch`；core group 的 API 路径是 `/api`，其他是 `/apis/<group>`
- **Version**：如 `v1`、`v1beta1`；同一资源可以有多个版本，apiserver 在内部统一用 Internal Version 处理
- **Kind** 与 **Resource** 同级：`Deployment` 是 Kind，`deployments` 是 Resource（URL 路径中的名称）

### 什么是 Scheme

Scheme 是 apiserver 中**资源类型注册表**，解决的核心问题是：收到一段 JSON/Protobuf 字节流时，怎么知道该反序列化成哪个 Go struct？

Scheme 注册了两种类型：
- **KnownType**：有版本的资源（绝大多数 K8s 资源）；通过 `scheme.AddKnownTypes` 注册
- **UnversionedType**：无版本资源（如 `metav1.Status`、`metav1.APIVersions`）；通过 `scheme.AddUnversionedTypes` 注册，序列化时不做版本转换

### Scheme 结构体定义

```go
// staging/src/k8s.io/apimachinery/pkg/runtime/scheme.go:46
type Scheme struct {
    gvkToType    map[schema.GroupVersionKind]reflect.Type       // GVK → Go Type（正向）
    typeToGVK    map[reflect.Type][]schema.GroupVersionKind     // Go Type → GVK（反向，一 Type 对多 GVK）
    unversionedTypes map[reflect.Type]schema.GroupVersionKind   // 无版本 Type → GVK
    unversionedKinds map[string]reflect.Type                    // Kind 名 → 无版本 Type

    fieldLabelConversionFuncs map[schema.GroupVersionKind]FieldLabelConversionFunc
    defaulterFuncs            map[reflect.Type]func(interface{})
    versionPriority           map[string][]string
    schemeName                string
}
```

四张 map 通过 Go 的 `reflect.Type` 作为 key/value，实现 O(1) 的正向（GVK→Type）和反向（Type→GVK）查找。`typeToGVK` 中一个 Type 可对应多个 GVK，因为同一个 Go struct 可以在多个版本中复用。

```
gvkToType      map[GroupVersionKind] → reflect.Type   ←→   typeToGVK   map[reflect.Type] → []GroupVersionKind
unversionedTypes  map[reflect.Type] → GroupVersionKind ←→   unversionedKinds  map[string] → reflect.Type
```

### 如何使用 Scheme

```go
// 1. 创建注册表
var Scheme = runtime.NewScheme()

// 2. 各包通过 init() 将自己的资源注册进去
func init() {
    _ = corev1.AddToScheme(Scheme)      // 注册 Pod、Service、ConfigMap 等
    _ = appsv1.AddToScheme(Scheme)      // 注册 Deployment、StatefulSet 等
    // ...
}

// 3. 获取解码器，用于将 JSON/Protobuf 反序列化为对应 Go struct
var Codecs = serializer.NewCodecFactory(Scheme)
var deserializer = Codecs.UniversalDeserializer()

// 4. 解码（自动识别 apiVersion/kind 并反序列化）
obj, gvk, err := deserializer.Decode(body, nil, nil)
```

第04章的 webhook server 中的 `runtimeScheme` + `codecs.UniversalDeserializer()` 就是这套机制的直接使用。

### InstallLegacyAPI → NewLegacyRESTStorage

```go
// pkg/controlplane/instance.go:541
func (m *Instance) InstallLegacyAPI(c *completedConfig, restOptionsGetter generic.RESTOptionsGetter,
        legacyRESTStorageProvider corerest.LegacyRESTStorageProvider) error {

    // 调用 NewLegacyRESTStorage，得到 restStorage 和 apiGroupInfo
    legacyRESTStorage, apiGroupInfo, err := legacyRESTStorageProvider.NewLegacyRESTStorage(restOptionsGetter)

    // 调用 m.GenericAPIServer.InstallLegacyAPIGroup 注册路由
    if err := m.GenericAPIServer.InstallLegacyAPIGroup(genericapiserver.DefaultLegacyAPIPrefix,
            &apiGroupInfo); err != nil {
        return fmt.Errorf("error in registering group versions: %v", err)
    }
    return nil
}
```

### NewLegacyRESTStorage：组装 apiGroupInfo

```go
// pkg/registry/core/rest/storage_core.go:104
func (c LegacyRESTStorageProvider) NewLegacyRESTStorage(restOptionsGetter generic.RESTOptionsGetter) (
        LegacyRESTStorage, genericapiserver.APIGroupInfo, error) {

    // 1. 构造 apiGroupInfo（绑定 scheme / codec / 版本优先级）
    apiGroupInfo := genericapiserver.APIGroupInfo{
        PrioritizedVersions:          legacyscheme.Scheme.PrioritizedVersionsForGroup(""),
        VersionedResourcesStorageMap: map[string]map[string]rest.Storage{},
        Scheme:                       legacyscheme.Scheme,
        ParameterCodec:               legacyscheme.ParameterCodec,
        NegotiatedSerializer:         legacyscheme.Codecs,
    }

    // 2. 为各资源创建 RESTStorage（以 configmap 为例）
    configMapStorage, err := configmapstore.NewREST(restOptionsGetter)
    // ...

    // 3. 为 pod 创建 RESTStorage（含子资源）
    podStorage, err := podstore.NewStorage(restOptionsGetter, ...)

    // 4. 组装 restStorageMap
    restStorageMap := map[string]rest.Storage{
        "pods":                    podStorage.Pod,
        "pods/attach":             podStorage.Attach,
        "pods/status":             podStorage.Status,
        "pods/log":                podStorage.Log,
        "pods/exec":               podStorage.Exec,
        "pods/portforward":        podStorage.PortForward,
        "pods/proxy":              podStorage.Proxy,
        "pods/binding":            podStorage.Binding,
        "bindings":                podStorage.LegacyBinding,
        "podTemplates":            podTemplateStorage,
        "replicationControllers":  controllerStorage.Controller,
        "replicationControllers/status": controllerStorage.Status,
        "services":                serviceRESTStorage,
        "services/proxy":          serviceRESTProxy,
        "configmaps":              configMapStorage,
        // ... 所有 v1 资源
    }

    // 5. 挂载到 apiGroupInfo
    apiGroupInfo.VersionedResourcesStorageMap["v1"] = restStorageMap

    return legacyRESTStorage, apiGroupInfo, nil
}
```

### RESTStorage 接口与 genericregistry.Store

`RESTStorage` 定义了一种资源如何 CRUD、如何与存储层交互。大多数资源的实现是 `*genericregistry.Store`，它封装了 etcd 读写的通用逻辑。

```go
// pkg/registry/core/configmap/storage/storage.go:33
type REST struct {
    *genericregistry.Store  // 嵌入通用 etcd 存储
}

func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
    store := &genericregistry.Store{
        NewFunc:                  func() runtime.Object { return &api.ConfigMap{} },
        NewListFunc:              func() runtime.Object { return &api.ConfigMapList{} },
        DefaultQualifiedResource: api.Resource("configmaps"),
        CreateStrategy:           configmap.Strategy,
        UpdateStrategy:           configmap.Strategy,
        DeleteStrategy:           configmap.Strategy,
    }
    options := &generic.StoreOptions{RESTOptions: optsGetter}
    store.CompleteWithOptions(options)
    return &REST{store}, nil
}
```

`*Strategy` 封装了业务校验逻辑（字段校验、默认值填充、PrepareForCreate/Update），与底层存储解耦。

### PodStorage：主资源 + 多子资源

Pod 与其他资源不同，它有大量子资源（exec / log / attach 等），因此返回的是 `PodStorage` 结构体而非单一 `*REST`：

```go
// pkg/registry/core/pod/storage/storage.go:51
type PodStorage struct {
    Pod                 *REST
    Binding             *BindingREST
    LegacyBinding       *LegacyBindingREST
    Eviction            *eviction.REST
    Status              *StatusREST
    EphemeralContainers *EphemeralContainersREST
    Log                 *podrest.LogREST
    Proxy               *podrest.ProxyREST
    Exec                *podrest.ExecREST
    Attach              *podrest.AttachREST
    PortForward         *podrest.PortForwardREST
}
```

每个子资源都是独立的 RESTStorage 实现，注册到 `restStorageMap` 时以 `"pods/exec"`、`"pods/log"` 等为 key，apiserver 路由时自动映射到对应 handler。

---

## §03 apiserver 中 Pod 数据的保存

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| genericregistry.Store.Create | [store.go](kubernetes/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go) | `Create:362` |
| DryRun 检查层 | [dryrun.go](kubernetes/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/dryrun.go) | `DryRunnableStorage.Create:36` |
| etcd3 真正写入 | [store.go](kubernetes/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go) | `store.Create:143` |
| Pod 业务校验 | [strategy.go](kubernetes/pkg/registry/core/pod/strategy.go) | `PrepareForCreate:82` |
| QoS 判定 | [qos.go](kubernetes/pkg/apis/core/helper/qos/qos.go) | `GetPodQOS:37` |

本节从 `genericregistry.Store.Create` 出发，跟踪一个 Pod 创建请求如何经过业务校验、QoS 计算，最终落地到 etcd。

### 总体调用链

```
REST handler（HTTP POST /api/v1/pods）
  │
  ▼ genericregistry.Store.Create           store.go:362
  │
  ├── BeginCreate（可选 hook，构造 finishCreate defer）
  │
  ├── BeforeCreate → strategy.PrepareForCreate  strategy.go:82
  │     ├── pod.Status = PodPending           设置初始状态
  │     ├── podutil.DropDisabledPodFields     去掉 feature gate 未开启的字段
  │     └── qos.GetPodQOS(pod)               计算 QoS 等级并写入 Status
  │
  ├── Storage.Create（DryRunnableStorage）   dryrun.go:36
  │     ├── DryRun == true → 只做 Get 检查 key 是否已存在，不写入
  │     └── DryRun == false → s.Storage.Create（真正写入）
  │           │
  │           ▼ etcd3.store.Create           etcd3/store.go:143
  │                 ├── PrepareObjectForStorage（序列化）
  │                 ├── s.client.KV.Txn（etcd 事务写入）
  │                 └── 写成功 → 解码返回对象 + 版本号
  │
  └── AfterCreate + Decorator（收尾 hook）
```

### BeforeCreate：业务校验与状态初始化

`BeforeCreate` 是 `genericregistry.Store` 在调用存储层之前统一执行的业务逻辑入口，它回调 `CreateStrategy` 的 `PrepareForCreate`：

```go
// pkg/registry/core/pod/strategy.go:82
func (podStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
    pod := obj.(*api.Pod)
    pod.Status = api.PodStatus{
        Phase:    api.PodPending,          // 新建 Pod 初始状态必须是 Pending
        QOSClass: qos.GetPodQOS(pod),      // 计算 QoS 等级
    }
    podutil.DropDisabledPodFields(pod, nil) // 去除 feature gate 关闭的字段
    applySeccompVersionSkew(pod)
}
```

新建 Pod 的 `Status.Phase` 由 apiserver 在此处强制设为 `Pending`，用户在 YAML 中写的 Status 字段会被忽略——这是 apiserver 对资源初始状态的强制管控，防止客户端伪造状态。

### GetPodQOS：QoS 三级判定

Kubernetes 用 QoS 在节点资源紧张时决定杀谁留谁。QoS 等级在 Pod 创建时由 apiserver 计算并写入 `Status.QOSClass`，后续 kubelet 直接读取该字段，不再重算。

判定逻辑（`pkg/apis/core/helper/qos/qos.go:37`）：

```
遍历所有容器的 Resources.Requests / Resources.Limits
  │
  ├── requests == 0 && limits == 0 → BestEffort   （不设任何资源限制）
  │
  ├── isGuaranteed（所有容器 requests == limits） → Guaranteed
  │
  └── 其他 → Burstable
```

| QoS 等级 | 触发条件 | OOM 优先级 |
|---------|---------|-----------|
| BestEffort | 没有任何 requests/limits | 最先被杀 |
| Burstable | requests < limits，或只设其中一个 | 中等 |
| Guaranteed | 所有容器 requests == limits，且均非零 | 最后被杀 |

### DryRunnableStorage：DryRun 语义在存储层的实现

```go
// staging/src/k8s.io/apiserver/pkg/registry/generic/registry/dryrun.go:36
func (s *DryRunnableStorage) Create(ctx context.Context, key string,
        obj, out runtime.Object, ttl uint64, dryRun bool) error {
    if dryRun {
        // 只查 etcd 看 key 是否已存在，不写入
        if err := s.Storage.Get(ctx, key, storage.GetOptions{}, out); err == nil {
            return storage.NewKeyExistsError(key, 0)
        }
        return s.copyInto(obj, out)
    }
    return s.Storage.Create(ctx, key, obj, out, ttl)  // 真正写入
}
```

`kubectl apply --dry-run=server` 时请求带 `dryRun=All`，apiserver 在此层短路：做完所有 Admission/Validation 之后，到达存储层时只检查 key 冲突、不实际落盘，让调用方获得与真实创建相同的校验反馈。

### etcd3.store.Create：真正的落盘

```go
// staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:143
func (s *store) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
    // 1. 序列化：将 Go struct 转成 Protobuf/JSON 字节流
    data, err := runtime.Encode(s.codec, obj)

    // 2. 计算存储 key（加前缀）
    key = path.Join(s.pathPrefix, key)

    // 3. 获取 TTL（租约）
    opts, err := s.ttlOpts(ctx, int64(ttl))

    // 4. 序列化元数据 (ResourceVersion = 0，表示 key 必须不存在)
    metadata, _ := s.transformer.TransformToStorage(data, authenticatedDataString(key))

    startTime := time.Now()
    // 5. etcd 事务写入：IF version == 0 THEN PUT
    txnResp, err := s.client.KV.Txn(ctx).If(
        clientv3.Compare(clientv3.Version(key), "=", 0),
    ).Then(
        clientv3.OpPut(key, string(metadata), opts...),
    ).Commit()

    if !txnResp.Succeeded {
        return storage.NewKeyExistsError(key, 0)   // key 已存在，并发冲突
    }
    // 6. 从响应中解码最终对象（带 ResourceVersion）
    putResp := txnResp.Responses[0].GetResponsePut()
    return decode(s.codec, s.versioner, data, out, putResp.Header.Revision)
}
```

etcd 事务（`IF version == 0 THEN PUT`）是防并发重复创建的关键：version == 0 意味着 key 不存在，事务原子地检查并写入，失败时返回 `KeyExistsError`。

---

## §04 apiserver 中的限流策略源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 限流 handler 选择 | [config.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/config.go) | `DefaultBuildHandlerChain:722` |
| MaxInFlight 实现 | [maxinflight.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/filters/maxinflight.go) | `WithMaxInFlightLimit:119` |
| APF hook 注册 | [config.go](kubernetes/staging/src/k8s.io/apiserver/pkg/server/config.go) | `GenericAPIServer.New:662` |

本节覆盖 apiserver 的四种限流机制，重点源码分析 MaxInFlightLimit 的 channel 队列实现，以及 APF（API Priority and Fairness）如何替代它。

### 四种限流机制概览

| 机制 | 粒度 | 特点 |
|------|------|------|
| **MaxInFlightLimit** | server 级别整体 | 最简单，分只读/修改两个 channel；v1.21 默认启用 |
| **Client 限流** | 客户端自身（如 client-go 默认 QPS=5） | 只能由各客户端自己控制，集群管理员无法干预 |
| **EventRateLimit** | event 请求（1.13+） | 以 Admission Webhook 形式集成在 apiserver 内部，可按用户/namespace/server 分级限制 |
| **APF**（API Priority and Fairness） | 请求级别，更细粒度 | v1.18 alpha，v1.20 beta；优先级队列 + FlowSchema；取代 MaxInFlightLimit |

### MaxInFlightLimit：channel 队列实现

**接入点**：`DefaultBuildHandlerChain` 中，根据是否启用 APF 选择走哪条限流路径：

```go
// staging/src/k8s.io/apiserver/pkg/server/config.go:727
if c.FlowControl != nil {
    handler = genericfilters.WithPriorityAndFairness(handler, c.LongRunningFunc, c.FlowControl)
} else {
    handler = genericfilters.WithMaxInFlightLimit(handler, c.MaxRequestsInFlight,
        c.MaxMutatingRequestsInFlight, c.LongRunningFunc)
}
```

`WithMaxInFlightLimit` 用两个有缓冲 channel 实现信号量：

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/maxinflight.go:119
func WithMaxInFlightLimit(handler http.Handler, nonMutatingLimit, mutatingLimit int,
        longRunningRequestCheck apirequest.LongRunningRequestCheck) http.Handler {

    // limit == 0 表示不限流，直接透传
    if nonMutatingLimit == 0 && mutatingLimit == 0 {
        return handler
    }

    // 构造两个 bool channel，容量 = limit
    var nonMutatingChan chan bool
    var mutatingChan chan bool
    if nonMutatingLimit != 0 {
        nonMutatingChan = make(chan bool, nonMutatingLimit)   // 只读请求队列
    }
    if mutatingLimit != 0 {
        mutatingChan = make(chan bool, mutatingLimit)          // 写操作请求队列
    }
    // ...
}
```

请求进入时的判断逻辑：

```
收到请求
  │
  ├── 是 long-running 请求（watch/proxy/debug）→ 直接放行，不占 slot
  │
  ├── 判断请求类型：isMutatingRequest = !nonMutatingRequestVerbs.Has(verb)
  │     ├── 修改请求 → 尝试写入 mutatingChan
  │     └── 只读请求 → 尝试写入 nonMutatingChan
  │
  └── select { case c <- true: ... }
        ├── 写入成功（有空位）→ 记录 metrics，执行 handler，defer 释放 slot
        └── default（队列已满）
              ├── 请求 group 包含 system:masters → 强制放行（cluster-admin 不被限流）
              └── 否则 → HTTP 429 + Retry-After: 1
```

`system:masters` 豁免只在队列满时才生效——有空位时所有请求都正常排队；队列满时，apiserver 认为来自 cluster-admin 的请求（如运维操作、紧急修复）不能被拒绝，否则在集群故障时管理员反而无法操作。

**限流参数**（启动参数）：
- `--max-requests-inflight`：只读请求并发上限（默认 400）
- `--max-mutating-requests-inflight`：修改请求并发上限（默认 200）

### APF（API Priority and Fairness）

APF 是 MaxInFlightLimit 的升级版，解决后者粒度太粗的问题：所有请求共用一个 channel，一个"坏邻居"可以吃掉全部配额。

APF 的核心设计：

```
FlowSchema（流量分类规则）
  │  按 user/group/resource/verb 匹配请求
  ▼
PriorityLevelConfiguration（优先级队列）
  │  每个优先级独立的并发配额 + 队列
  ▼
公平队列调度（WFQ）
  │  同优先级内按 FlowDistinguisher（如 namespace/user）公平分配
  ▼
handler.ServeHTTP
```

- `FlowSchema` + `PriorityLevelConfiguration` 是 CRD 对象，可在运行时动态配置
- apiserver 启动时写入一批 default namespace 的 FlowSchema（如 `exempt`、`system`、`workload-high` 等）
- APF hook 在 `GenericAPIServer.New` 中通过 `PostStartHook` 注册（`priority-and-fairness-filter`，config.go:663），server 启动后自动生效

**与 MaxInFlightLimit 的关系**：两者互斥，由 `c.FlowControl != nil` 决定走哪条路；生产环境 v1.20+ 建议启用 APF。


## §05 apiserver 重要对象和功能总结

本节为全章总结。各知识点的详细分析见对应章节，此处仅梳理各模块在整体请求链路中的位置。

### apiserver 的三个 server

| server | 职责 | 路由前缀 |
|--------|------|---------|
| apiExtensionsServer | 处理 CRD 请求 | `/apis/<crd-group>/` |
| kubeAPIServer | 处理内置资源（Pod/Service/Deployment 等） | `/api/v1/`、`/apis/apps/v1/` 等 |
| aggregatorServer | 聚合外部 APIServer（metrics-server 等） | 代理转发 |

三个 server 均通过 `GenericAPIServer.New` 构造（见 §01），以委托链串联：aggregator → kubeAPI → apiExtensions → notFound。

### 一个请求的完整生命周期

中间件在 `DefaultBuildHandlerChain` 中以包裹方式注册，实际执行顺序与代码顺序相反：

```
API HTTP Request
  │
  ▼ DefaultBuildHandlerChain（中间件层，执行顺序从上到下）
  │   ├── PanicRecovery / RequestInfo / WaitGroup / Timeout / CORS
  │   ├── Authentication（身份认证）          config.go:748   见第03章
  │   ├── Audit（审计）                       config.go:740
  │   ├── 限流：APF / MaxInFlightLimit        config.go:727   见 §04
  │   └── Authorization（鉴权）               config.go:724   见第03章
  │
  ▼ 路由到对应资源的 RESTStorage handler
  │   ├── Admission（准入：Mutating → Schema验证 → Validating）  见第03/04章
  │   └── genericregistry.Store.Create/Update/Delete/Get
  │         ├── BeforeCreate → Strategy.PrepareForCreate（业务校验）  见 §03
  │         └── Storage.Create → etcd3.store.Create（落盘）          见 §03
  │
  ▼ 返回响应（含 ResourceVersion）
```

### 核心对象速查

| 对象 | 作用 | 详见 |
|------|------|------|
| `GenericAPIServer` | 三个 server 的共同基座，含 handler chain / PostStartHook | §01 |
| `Scheme` | GVK ↔ Go Type 双向注册表（四张 map） | §02 |
| `RESTStorage` / `genericregistry.Store` | 每种资源的 CRUD 实现，嵌 etcd 逻辑 | §02 |
| `restStorageMap` | 路由 key → RESTStorage 的映射，驱动 URL 路由注册 | §02 |
| `Strategy`（PrepareForCreate 等） | 各资源的业务校验/默认值逻辑，与存储解耦 | §03 |
| `DryRunnableStorage` | 在存储层实现 `--dry-run=server` 语义 | §03 |
| `WithMaxInFlightLimit` | channel 信号量限流，v1.21 默认；v1.20+ 可换 APF | §04 |




































