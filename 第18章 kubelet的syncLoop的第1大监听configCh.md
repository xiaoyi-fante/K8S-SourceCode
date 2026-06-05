# 第18章 kubelet 的 syncLoop 第1大监听 configCh

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 18 章 — kubelet 的 syncLoop 第1大监听 configCh
> **源码入口**: `pkg/kubelet/kubelet.go` / `pkg/kubelet/config/`

---

## 核心机制一览

1. **syncLoopIteration 是 kubelet 的事件分发中枢**：从 5 类事件循环（7 个 chan）读取数据，将 pod 分派给对应处理程序。configCh 负责将来自 apiserver/file/http 三个来源的 pod 变更分派到 handler 回调（Add/Update/Remove/Reconcile/Delete）。

2. **三通道统一汇聚架构**：apiserver、file、http 三个 pod 来源分别调用 `NewSourceApiserver` / `NewSourceFile` / `NewSourceURL` 注册，各自构建 `send` 方法把 pod 写入 `updates chan`；PodConfig 的 `Channel` 方法为每个 source 创建独立 `newChannel`，然后通过 `mux.listen` 将其 merge 到同一个 `podStorage`。

3. **podStorage.Merge 是三路数据的聚合点**：每个 source 的 update 都调用 `podStorage.Merge(source, change)`，内部调用 `merge` 方法计算出 `adds/updates/deletes/removes/reconciles` 五类变更，再在 `PodConfigNotificationIncremental` 模式下逐类写入 `s.updates` chan 发给 syncLoop。

4. **UndeltaStore 是 store 与 updates chan 的桥梁**：`store.Add/Update/Delete` 每次操作后都调用 `PushFunc`（即构造时传入的 `send` 方法），`send` 将当前 store 中所有 pod 拍平成 `kubetypes.PodUpdate{Op: SET}` 发送。这样 store 与 updates chan 始终保持同步。

5. **apiserver 通道的两个前置条件**：① 先等待 `nodeHasSynced`（node informer 同步完成）再启动 Reflector 监听 apiserver pods；② 用 `NewListWatchFromClient` 通过 client-go 创建 ListerWatcher，指定 `nodeName` 做 field selector 过滤只属于本节点的 pod。

6. **file 通道的双工机制**：`run` 函数用 ticker 定期执行 `listConfig` 全量扫描，同时用 `startWatch`（基于 fsnotify）监听文件系统变更事件，两条路径都最终调用 `extractFromFile` 解析 yaml/json → runtime.Decode → 校验 → 写 store，store 的 PushFunc 触发 updates chan。

---

## 全章调用链总图

```
kubelet.Run
  │
  └─ k.Run(podCfg.Updates())  ← 把 updates chan 传入 syncLoop
       │
       └─ syncLoop → syncLoopIteration (kubelet.go:1926)
            │
            case configCh:
              switch u.Op {
                ADD/UPDATE/DELETE → handler.HandlePodUpdates (kubelet.go:2122)
                                       └─ kl.dispatchWork(pod, SyncPodUpdate, ...)
                REMOVE → handler.HandlePodRemoves
                RECONCILE → handler.HandlePodReconcile
                SET → handler.HandlePodSyncs
              }

configCh 的 updates chan 来自 PodConfig.Updates()
  │
  └─ podStorage.updates (config.go:102)
       │
       填充者: podStorage.Merge (config.go:150)
         │
         ├─ source=apiserver: NewSourceApiserver (config/apiserver.go:31)
         │    └─ newSourceApiserverFromLW (config/apiserver.go:37)
         │         ├─ 等待 nodeHasSynced（node informer 同步完）
         │         ├─ NewListWatchFromClient (client-go)
         │         │    fieldSelector: nodeName=<本节点>
         │         └─ send = func(objs) { updates <- PodUpdate{Pods, Op:SET} }
         │              NewUndeltaStore(send, keyFunc)
         │              NewReflector(lw, &v1.Pod{}, store, ...)
         │              go r.Run(wait.NeverStop)
         │
         ├─ source=file: NewSourceFile (config/file.go:63)
         │    └─ sourceFile.run()
         │         ├─ listTicker: listConfig() → extractFromDir/extractFromFile
         │         └─ startWatch(): doWatch() → fsnotify
         │              consumeWatchEvent(e):
         │                Add/Modify → extractFromFile → store.Add
         │                Delete → store.Delete
         │              store.Add → UndeltaStore.PushFunc → send → updates chan
         │
         └─ source=http: NewSourceURL (config/http.go:46)
              └─ sourceURL.run() → ticker → extractFromURL()
                   HTTP GET → data → tryDecodeSinglePod → tryDecodePodList
                   → updates <- PodUpdate{Pods, Op:SET}

PodConfig 聚合层 (config/config.go):
  NewPodConfig (config.go:70)         → PodConfig{mux, storage, updates}
  Channel(source) (config.go:84)      → mux.Channel(source) → newChannel
  mux.listen(source, newChannel):
    for update := range listenChannel {
        m.merger.Merge(source, update)  → podStorage.Merge
    }
  podStorage.Merge (config.go:150)
    → merge() → adds/updates/deletes/removes/reconciles
    → PodConfigNotificationIncremental 模式下逐类 s.updates <- *adds 等
```

---

## §01 syncLoop 的 configCh 中的 apiserver 通信流程

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| syncLoop 主循环 | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `syncLoopIteration:1926` |
| configCh handler 回调 | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `HandlePodUpdates:2122` |
| 三通道注册入口 | [kubelet/kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `makePodSourceConfig` |
| apiserver 通道 | [config/apiserver.go](kubernetes/pkg/kubelet/config/apiserver.go) | `NewSourceApiserver:31` |
| PodConfig 构建 | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `NewPodConfig:70` |

### syncLoopIteration：5 类事件 7 个 chan

```
configCh   — pod 配置变更（apiserver/file/http 三源合并）
plegCh     — PLEG 产生的 pod 状态变化 event
syncCh     — 等待 sync 的 pod
housekeepingCh — 触发清理 pod
health manager：
  startupManager
  readinessManager
  livenessManager
```

configCh 收到 `PodUpdate` 后，按 `Op` 分发：

```go
// pkg/kubelet/kubelet.go:1926
func (kl *Kubelet) syncLoopIteration(configCh <-chan kubetypes.PodUpdate, ...) bool {
    select {
    case u, open := <-configCh:
        switch u.Op {
        case kubetypes.ADD:
            handler.HandlePodAdditions(u.Pods)
        case kubetypes.UPDATE:
            handler.HandlePodUpdates(u.Pods)
        case kubetypes.REMOVE:
            handler.HandlePodRemoves(u.Pods)
        case kubetypes.RECONCILE:
            handler.HandlePodReconcile(u.Pods)
        case kubetypes.DELETE:
            // DELETE treated as UPDATE for graceful deletion
            handler.HandlePodUpdates(u.Pods)
        case kubetypes.SET:
            // 不支持
        }
    // ...其他 chan...
    }
}
```

`HandlePodUpdates` 底层调用 `dispatchWork`，将 pod 分发给 podWorker 异步处理（SyncPodUpdate → syncPod）。

### configCh 的 PodUpdate 来源

configCh 就是 kubelet 的 `Run` 方法中传入的 `podCfg.Updates()`：

```go
// cmd/kubelet/app/server.go
go k.Run(podCfg.Updates())
```

`PodConfig.Updates()` 返回的就是 `podStorage.updates` 这个 channel：

```go
// pkg/kubelet/config/config.go:102
func (c *PodConfig) Updates() <-chan kubetypes.PodUpdate {
    return c.updates
}
```

### 三通道注册：makePodSourceConfig

`makePodSourceConfig`（`pkg/kubelet/kubelet.go`）根据配置决定启用哪些 source：

```go
// define file config source
if kubeCfg.StaticPodPath != "" {
    config.NewSourceFile(kubeCfg.StaticPodPath, nodeName, kubeCfg.FileCheckFrequency.Duration, cfg.Channel(kubetypes.FileSource))
}
// define url config source
if kubeCfg.StaticPodURL != "" {
    config.NewSourceURL(kubeCfg.StaticPodURL, manifestURLHeader, nodeName, kubeCfg.HTTPCheckFrequency.Duration, cfg.Channel(kubetypes.HTTPSource))
}
// apiserver
if kubeDeps.KubeClient != nil {
    config.NewSourceApiserver(kubeDeps.KubeClient, nodeName, nodeHasSynced, cfg.Channel(kubetypes.ApiserverSource))
}
```

### 监听 apiserver pod 变更：NewSourceApiserver

```go
// pkg/kubelet/config/apiserver.go:31
func NewSourceApiserver(c clientset.Interface, nodeName types.NodeName, updates chan<- interface{}) {
    lw := cache.NewListWatchFromClient(c.CoreV1().RESTClient(), "pods", metav1.NamespaceAll,
        fields.OneTermEqualSelector("spec.nodeName", string(nodeName)))
    // ...
    newSourceApiserverFromLW(lw, updates)
}
```

`NewListWatchFromClient` 创建的是标准 client-go ListerWatcher，通过 `spec.nodeName` field selector 只 watch 调度到本节点的 pod，通信使用 HTTP/2 长连接（Informer 内部）。

### 等待 nodeHasSynced 再启动 Reflector

```go
// pkg/kubelet/config/apiserver.go:37
func newSourceApiserverFromLW(lw cache.ListerWatcher, updates chan<- interface{}) {
    send := func(objs []interface{}) {
        var pods []*v1.Pod
        for _, o := range objs { pods = append(pods, o.(*v1.Pod)) }
        updates <- kubetypes.PodUpdate{Pods: pods, Op: kubetypes.SET, Source: kubetypes.ApiserverSource}
    }
    // 等待 node sync 结束后再 watch apiserver pods
    go func() {
        for {
            if nodeHasSynced() { break }
            time.Sleep(WaitForAPIServerSyncPeriod)
        }
        klog.InfoS("Watching apiserver")
        newSourceApiserverFromLW(lw, updates)
    }()
}
```

`nodeHasSynced` 类型为 `cache.InformerSynced`，其定义为 `kubeInformers.Core().V1().Nodes().Informer().HasSynced`。Informer 带本地缓存，重启后需先 List 获取全量数据填充缓存，`HasSynced` 为 true 表示首次 List 完成，此时再开始 watch apiserver pods 才是安全的。

### UndeltaStore：store → updates chan 的桥梁

`newSourceApiserverFromLW` 构建的是 `UndeltaStore`，其 `PushFunc` 就是上面的 `send`：

```go
store := cache.NewUndeltaStore(send, cache.MetaNamespaceKeyFunc)
r := cache.NewReflector(lw, &v1.Pod{}, store, ...)
go r.Run(wait.NeverStop)
```

`UndeltaStore.Add(obj)` 调用逻辑：先写 Store，再调 `PushFunc(store.List())`，把当前 store 中**所有** pod 以 `Op: SET` 形式整体发出。这是 apiserver 通道与 file/http 通道的设计差异：apiserver 用 Informer Reflector 驱动增量事件转成全量 SET，file/http 自行实现定时 poll 或 fsnotify 事件。

---

## §02 syncLoop 的 configCh 中的 file 源码

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| file 通道注册 | [config/file.go](kubernetes/pkg/kubelet/config/file.go) | `NewSourceFile:63` |
| 轮询+监听双路 | [config/file.go](kubernetes/pkg/kubelet/config/file.go) | `run:92` |
| 解析文件/目录 | [config/file.go](kubernetes/pkg/kubelet/config/file.go) | `listConfig:121` / `extractFromFile:200` |
| fsnotify 监听 | [config/file_linux.go](kubernetes/pkg/kubelet/config/file_linux.go) | `startWatch` |

### newSourceFile：构建 sourceFile 对象

```go
// pkg/kubelet/config/file.go:63
func NewSourceFile(path string, nodeName types.NodeName, period time.Duration, updates chan<- interface{}) {
    // ...
    config := newSourceFile(path, nodeName, period, updates)
    config.run()
}
```

`sourceFile` 构造时同样创建 `send`（同 apiserver 模式），`store = cache.NewUndeltaStore(send, keyFunc)`，然后返回 `&sourceFile{path, nodeName, period, store, updates, watchEvents: make(chan *watchEvent, eventBufferLen)}`。

### run：ticker + watchEvents 双路监听

```go
// pkg/kubelet/config/file.go:92
func (s *sourceFile) run() {
    listTicker := time.NewTicker(s.period)
    go func() {
        // 立即执行一次，加速启动
        if err := s.listConfig(); err != nil { ... }
        for {
            select {
            case <-listTicker.C:
                if err := s.listConfig(); err != nil { ... }
            case e := <-s.watchEvents:
                if err := s.consumeWatchEvent(e); err != nil { ... }
            }
        }
    }()
}
```

ticker 走 `listConfig` 全量扫描；`watchEvents` 走 `consumeWatchEvent` 增量处理。两路最终都落到 store，store 的 PushFunc 触发 send 写 updates chan。

### listConfig：path 类型决定解析函数

```go
// pkg/kubelet/config/file.go:121
func (s *sourceFile) listConfig() error {
    path := s.path
    statInfo, err := os.Stat(path)
    // ...
    switch {
    case statInfo.Mode().IsDir():
        pods, err := s.extractFromDir(path)  // 遍历目录，每个文件 extractFromFile
        // ...
        return s.replaceStore(pods...)
    case statInfo.Mode().IsRegular():
        pod, err := s.extractFromFile(path)
        // ...
        return s.replaceStore(pod)
    }
}
```

### extractFromFile：yaml → runtime.Decode → Pod

```go
// pkg/kubelet/config/file.go:200
func (s *sourceFile) extractFromFile(filename string) (pod *v1.Pod, err error) {
    defaultFn := func(pod *api.Pod) error {
        return s.applyDefaults(pod, filename)
    }
    parsed, pod, podErr := tryDecodeSinglePod(data, defaultFn)
    // ...
}
```

`tryDecodeSinglePod` 核心：
1. `utilyaml.ToJSON(data)` — yaml 转 json
2. `runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), json)` — 反序列化成 runtime.Object
3. `obj.(*api.Pod)` — 断言为 Pod
4. `defaultFn(pod)` — 设置默认值并校验
5. `validation.ValidatePodCreate(newPod, ...)` — 校验
6. `k8s_api_v1.Convert_core_Pod_To_v1_Pod(newPod, v1Pod, nil)` — 转为 v1 Pod 返回

### applyDefaults：为 static pod 设置关键字段

`applyDefaults` 完成三件事：
1. **生成 UID**：`md5.New()` 对 `host:nodeName` + `file:source` 做 Hash，`hex.EncodeToString(hasher.Sum(nil)[0:])` → `pod.UID`（保证同节点同文件的 pod UID 稳定）
2. **生成 Name**：`generatePodName(pod.Name, nodeName)`
3. **添加 NoExecute Toleration**：static pod 不被 Evict，在 `pod.Spec.Tolerations` 中添加 `Operator=Exists, Effect=NoExecute`

污点 `Operator=Exists` 匹配任意 key/value，无论节点上有什么污点，static pod 都不会被驱逐。

### fsnotify 监听：startWatch/doWatch

```go
// pkg/kubelet/config/file_linux.go: startWatch()
func (s *sourceFile) startWatch() {
    // backoff 重试
    go wait.Forever(func() {
        if err := s.doWatch(); err != nil { backOff.Next(...) }
    }, retryPeriod)
}

// doWatch: 用 fsnotify 监听 path
w, _ := fsnotify.NewWatcher()
defer w.Close()
w.Add(s.path)
for {
    select {
    case event := <-w.Events:
        s.produceWatchEvent(&event)
    case err := <-w.Errors:
        // ...
    }
}
```

`produceWatchEvent` 把 fsnotify 事件类型映射为 pod 事件类型：
- `Create/Write/Chmod` → `podAdd/podModify`
- `Remove/Rename` → `podDelete`
- 忽略 `.` 开头文件

`consumeWatchEvent` 根据 eventType：
- `podAdd/podModify` → `extractFromFile` → `store.Add`
- `podDelete` → `store.Delete`

---

## §03 syncLoop 的 configCh 中的 http 源码

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| http 通道注册 | [config/http.go](kubernetes/pkg/kubelet/config/http.go) | `NewSourceURL:46` |
| 轮询主循环 | [config/http.go](kubernetes/pkg/kubelet/config/http.go) | `run:61` |
| 拉取并解析 | [config/http.go](kubernetes/pkg/kubelet/config/http.go) | `extractFromURL:85` |

### NewSourceURL：http client + ticker

触发条件：`kubeCfg.StaticPodURL`（命令行 `--manifest-url`）不为空。

```go
// pkg/kubelet/config/http.go:46
func NewSourceURL(url string, header http.Header, nodeName types.NodeName,
    period time.Duration, updates chan<- interface{}) {
    config := &sourceURL{
        url:      url,
        header:   header,
        nodeName: nodeName,
        updates:  updates,
        data:     nil,
        client:   &http.Client{Timeout: 10 * time.Second},
    }
    go wait.Until(config.run, period, wait.NeverStop)
}
```

### run → extractFromURL：HTTP GET + 解析

```go
// pkg/kubelet/config/http.go:61
func (s *sourceURL) run() {
    if err := s.extractFromURL(); err != nil {
        // failureLogs 控制日志级别，避免重复失败刷日志
    }
}
```

`extractFromURL` 流程：
1. `http.NewRequest("GET", s.url, nil)` + 设置 Header
2. `s.client.Do(req)` → resp
3. `ioutil.ReadAtMost(resp.Body, maxConfigLength)` — 读取 body（有大小上限）
4. **短路优化**：`bytes.Equal(data, s.data)` — 若 data 未变，直接 return nil（避免重复解析）
5. `tryDecodeSinglePod(data, ...)` — 先尝试解析为单个 pod
6. 若失败则 `tryDecodePodList(data, ...)` — 解析为 PodList，遍历写入 updates

与 file 通道不同，http 通道不使用 store+UndeltaStore，而是直接构造 `kubetypes.PodUpdate{Pods: pods, Op: kubetypes.SET}` 发送到 `s.updates` chan。

---

## §04 syncLoop 的 configCh 中的 merge 逻辑

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| PodConfig 构建 | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `NewPodConfig:70` |
| Channel 方法 | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `Channel:84` |
| podStorage.Merge | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `Merge:150` |
| merge 内部计算 | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `merge:209` |
| updatePodsFunc | [config/config.go](kubernetes/pkg/kubelet/config/config.go) | `filterInvalidPods:319` |

### PodConfig.Channel：为每个 source 创建独立 newChannel

```go
// pkg/kubelet/config/config.go:84
func (c *PodConfig) Channel(source string) chan<- interface{} {
    c.sourcesLock.Lock()
    defer c.sourcesLock.Unlock()
    c.sources.Insert(source)
    return c.mux.Channel(source)
}
```

`c.mux.Channel(source)` 的逻辑：
- source 为新值：创建 `newChannel = make(chan interface{})`，启动 `go wait.Until(m.listen(source, newChannel), 0, wait.NeverStop)` 监听合并
- source 已存在：直接返回缓存中的 channel

```go
// listen：从 listenChannel 读取，调用 Merge 聚合
func (m *Mux) listen(source string, listenChannel <-chan interface{}) {
    for update := range listenChannel {
        m.merger.Merge(source, update)
    }
}
```

### podStorage.Merge：聚合三路数据

```go
// pkg/kubelet/config/config.go:150
func (s *podStorage) Merge(source string, change interface{}) error {
    s.updateLock.Lock()
    defer s.updateLock.Unlock()

    adds, updates, deletes, removes, reconciles := s.merge(source, change)

    switch s.mode {
    case PodConfigNotificationIncremental:
        if len(removes.Pods) > 0 { s.updates <- *removes }
        if len(adds.Pods) > 0    { s.updates <- *adds }
        if len(updates.Pods) > 0 { s.updates <- *updates }
        if len(deletes.Pods) > 0 { s.updates <- *deletes }
        // firstSet：source 第一次出现且无任何变更，发一个空 adds 告知 kubelet source 已就绪
        if firstSet && len(adds.Pods) == 0 && len(updates.Pods) == 0 && len(deletes.Pods) == 0 {
            s.updates <- *adds
        }
        if len(reconciles.Pods) > 0 { s.updates <- *reconciles }
    }
}
```

### merge 内部：updatePodsFunc 计算五类变更

`merge` 中核心是 `updatePodsFunc`，处理 `kubetypes.SET` 类型的 update（file/apiserver/http 都发 SET）：

```go
case kubetypes.SET:
    pods = make(map[types.UID]*v1.Pod)
    // updatePodsFunc: 比较新旧 pod，分类到 add/update/delete/reconcile
    updatePodsFunc(update.Pods, oldPods, pods)
    // ...
```

`filterInvalidPods` 先对每个 source 的 pod 做去重（同 source 内不允许重复 pod name），过滤非法 pod，然后给每个 pod 的 Annotations 打上 `kubernetes.io/config.source = source`（标记来源）。

`checkAndUpdatePod(existing, ref)` 判断 pod 是否需要 update/reconcile/gracefulDelete：

```go
// pkg/kubelet/config/config.go:419
func checkAndUpdatePod(existing, ref *v1.Pod) (needUpdate, needReconcile, needGracefulDelete bool) {
    if podsDifferSemantically(existing, ref) {
        needUpdate = true
    } else if !isAnnotationMapEqual(existing.Annotations, ref.Annotations) {
        needReconcile = true
    } else if ref.DeletionTimestamp != nil {
        needGracefulDelete = true
    }
}
```

`podsDifferSemantically` 排除 local annotations（如 `kubectl.kubernetes.io/last-applied-configuration`）后比较 pod spec 是否变化，避免本地 annotation 变化触发不必要的 syncPod。

### merge 最终结果发出

`merge` 函数将 `adds/updates/deletes/removes/reconciles` 各自包装成对应 Op 的 `kubetypes.PodUpdate`：

```go
adds     = &kubetypes.PodUpdate{Op: kubetypes.ADD,       Pods: copyPods(addPods), Source: source}
updates  = &kubetypes.PodUpdate{Op: kubetypes.UPDATE,    Pods: copyPods(updatePods), ...}
deletes  = &kubetypes.PodUpdate{Op: kubetypes.DELETE,    Pods: copyPods(deletePods), ...}
removes  = &kubetypes.PodUpdate{Op: kubetypes.REMOVE,    Pods: copyPods(removePods), ...}
reconciles = &kubetypes.PodUpdate{Op: kubetypes.RECONCILE, ...}
```

`Merge` 方法在 `PodConfigNotificationIncremental` 模式下把这五类依次写入 `s.updates` chan，最终由 `syncLoopIteration` 的 `case configCh:` 读取并按 Op 分发给 handler。
