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

---

## §04 apiserver 中的限流策略源码解读

---

## §05 apiserver 重要对象和功能总结
