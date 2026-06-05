# 第20章 kubelet中内置的cadvisor

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 20 章 — kubelet 中内置的 cadvisor
> **源码入口**: `pkg/kubelet/cadvisor/cadvisor_linux.go`

## 核心机制一览

1. **cadvisor 内嵌于 kubelet**：cadvisor 不是独立进程，而是作为一个库嵌入在 kubelet 内部，通过 `kubeDeps.CAdvisorInterface` 传递给 kubelet 使用，提供节点上所有容器的资源使用情况（CPU、内存、网络、文件系统等）。

2. **插件注册机制（init 副作用）**：`cadvisor_linux.go` 的 `init()` 通过 blank import 触发三个 install 包（containerd/install、crio/install、systemd/install）的 `init()`，每个 `init()` 调用 `container.RegisterPlugin` 将自身注册进 `plugins map[string]Plugin`。这是 Go 的"注册表"模式——只要导入包，副作用自动完成注册，调用方无需感知实现细节。

3. **cadvisor.New 构造 Manager**：在 kubelet `run()` 中调用 `cadvisor.New`，内部依次获取 fsInfo（挂载点信息）、machineInfo（机器硬件信息），构造 `manager` 结构体并返回。`manager` 是 cadvisor 的核心对象，持有 containers map、memoryCache、containerWatchers 等字段。

4. **Start 启动四条 goroutine**：`cadvisorManager.Start()`（即 `manager.Start`，manager.go:271）通过 `InitializePlugins` 获取 containerWatchers 列表，然后启动四条并发 goroutine：① `watchForNewContainers`（inotify 监听 cgroup 目录，发现新/删除容器）② `watchForNewOoms`（解析 /dev/kmsg 内核日志，检测 OOM Kill 事件）③ `globalHousekeeping`（定期探测子容器目录，刷新容器列表）④ `updateMachineInfo`（定时刷新机器信息供 kubelet 查询）。

5. **rawContainerWatcher 双路径产生 ContainerEvent**：`rawContainerWatcher.Start`（raw/watcher.go:71）遍历 cgroupPaths 的顶级目录调用 `watchDirectory`，`watchDirectory` 读取初始子目录并设置 inotify 监听；外部 goroutine 的 `processEvent` 处理内核 inotify 事件，将目录创建映射为 `ContainerAdd`、目录删除映射为 `ContainerDelete`，统一发往 `m.eventsChannel`。

6. **watchForNewContainers 消费者创建资源采集**：消费 `eventsChannel` 中的 ContainerEvent；`ContainerAdd` → `createContainer`（为该容器启动独立的数据采集 goroutine，读 `/proc/<pid>/` 统计文件）；`ContainerDelete` → `destroyContainer`（停止采集并清理）。

7. **OOM 检测流程**：`watchForNewOoms`（manager.go:1202）创建 oomParser，调用 `StreamOoms`（oomparser.go:117）在独立 goroutine 中逐行解析 /dev/kmsg，识别 "invoked oom-killer" 和 containerRegexp 正则匹配容器进程，通过 outStream channel 传递 OomInstance；消费侧为每个 OomInstance 发布两个 Event：`EventOom`（容器发生 OOM）和 `EventOomKill`（含 Pid/ProcessName）。

---

## 全章调用链总图

```
kubelet run()
  │
  ▼ cadvisor.New(imageFsInfoProvider, rootPath, ...) — cadvisor_linux.go:82
  │   ├─ 触发三个 install 包 init(): containerd/crio/systemd → RegisterPlugin
  │   ├─ 获取 fsInfo (挂载点信息)
  │   ├─ 获取 machineInfo (机器硬件信息)
  │   └─ 返回 &cadvisorClient{Manager: manager}
  │
  ▼ kubeDeps.CAdvisorInterface 赋值给 kubelet.cadvisor
  │
  ▼ initializeRuntimeDependentModules() — kubelet.go:1371
  │   └─ kl.cadvisor.Start() → cadvisorClient.Start() — cadvisor_linux.go:128
  │             └─ cc.Manager.Start() — manager.go:271
  │                   │
  │                   ├─ InitializePlugins() — factory.go:157
  │                   │     遍历 plugins map → plugin.Register() → containerWatchers
  │                   │
  │                   ├─ raw.Register() + raw.NewRawContainerWatcher() → rawWatcher
  │                   │
  │                   ├─ go watchForNewContainers(quit) — manager.go:1139
  │                   │     ├─ 遍历 containerWatchers → watcher.Start(eventsChannel)
  │                   │     │     └─ rawContainerWatcher.Start — watcher.go:71
  │                   │     │           ├─ watchDirectory (inotify 监听 cgroup 目录)
  │                   │     │           └─ processEvent → ContainerAdd/ContainerDelete
  │                   │     └─ 消费 eventsChannel
  │                   │           ├─ ContainerAdd → createContainer
  │                   │           └─ ContainerDelete → destroyContainer
  │                   │
  │                   ├─ go watchForNewOoms() — manager.go:1202
  │                   │     ├─ oomparser.New() + StreamOoms(outStream) — oomparser.go:117
  │                   │     │     解析 /dev/kmsg → OomInstance → outStream
  │                   │     └─ 消费 outStream → AddEvent(EventOom) + AddEvent(EventOomKill)
  │                   │
  │                   ├─ go globalHousekeeping(quit) — manager.go:376
  │                   │     定期 detectSubcontainers("/") 刷新容器列表
  │                   │
  │                   └─ go updateMachineInfo(quit) — manager.go:354
  │                         定时 machine.Info() 刷新机器信息
  │
  ▼ kubelet 调用 kl.cadvisor.MachineInfo() — kubelet.go:...
        → GetMachineInfo() — manager.go:813
              m.machineInfo.Clone() 返回深拷贝
```

---

## §01. kubelet 中内置的 cadvisor

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| cadvisor 对象构造 | [cadvisor_linux.go](kubernetes/pkg/kubelet/cadvisor/cadvisor_linux.go) | `New:82` |
| 插件注册（init 副作用） | [cadvisor_linux.go](kubernetes/pkg/kubelet/cadvisor/cadvisor_linux.go) | `init:62` |
| cadvisor Start 入口 | [cadvisor_linux.go](kubernetes/pkg/kubelet/cadvisor/cadvisor_linux.go) | `Start:128` |
| manager.New 构造 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `New:149` |
| manager.Start | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `Start:271` |
| 插件遍历注册 watcher | [factory.go](kubernetes/vendor/github.com/google/cadvisor/container/factory.go) | `InitializePlugins:157` |
| 插件写入 map | [factory.go](kubernetes/vendor/github.com/google/cadvisor/container/factory.go) | `RegisterPlugin:133` |
| kubelet 启动 cadvisor | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `initializeRuntimeDependentModules:1371` |
| MachineInfo 查询 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `GetMachineInfo:813` |
| 新容器监听 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `watchForNewContainers:1139` |
| OOM 检测 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `watchForNewOoms:1202` |
| OOM 日志流解析 | [oomparser.go](kubernetes/vendor/github.com/google/cadvisor/utils/oomparser/oomparser.go) | `StreamOoms:117` |
| cgroup inotify 监听 | [watcher.go](kubernetes/vendor/github.com/google/cadvisor/container/raw/watcher.go) | `Start:71` |
| inotify 事件处理 | [watcher.go](kubernetes/vendor/github.com/google/cadvisor/container/raw/watcher.go) | `processEvent:179` |
| 全局垃圾清理 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `globalHousekeeping:376` |
| 机器信息定时更新 | [manager.go](kubernetes/vendor/github.com/google/cadvisor/manager/manager.go) | `updateMachineInfo:354` |

### cadvisor 的作用与 kubelet 的关系

cadvisor（Container Advisor）的作用是**提供运行容器的资源使用情况和性能特征**。它不作为独立进程运行，而是以库的形式内嵌在 kubelet 中，构造时调用 `cadvisor.New` 生成 `kubeDeps.CAdvisorInterface` 对象，后续赋值给 `kubelet.cadvisor` 字段供各处调用。

### 插件注册机制（init 副作用）

`cadvisor_linux.go` 的 `init()` 通过 blank import 三个 install 包，但这几个包在代码中**只导入不使用**（`_` 导入）：

```go
// pkg/kubelet/cadvisor/cadvisor_linux.go — init 副作用
_ "github.com/google/cadvisor/container/containerd/install"
_ "github.com/google/cadvisor/container/crio/install"
_ "github.com/google/cadvisor/container/systemd/install"
```

每个 install 包的 `init()` 调用 `container.RegisterPlugin`，将自身注册进全局 `plugins map[string]Plugin`：

```go
// vendor/github.com/google/cadvisor/container/containerd/install/install.go
func init() {
    err := container.RegisterPlugin("containerd", containerd.NewPlugin())
    // ...
}
```

```go
// vendor/github.com/google/cadvisor/container/factory.go:133
func RegisterPlugin(name string, plugin Plugin) error {
    pluginsLock.Lock()
    defer pluginsLock.Unlock()
    if _, found := plugins[name]; found {
        return fmt.Errorf("Plugin %q was registered twice", name)
    }
    plugins[name] = plugin
    return nil
}
```

这是 Go 的"注册表"模式：只要导入包，副作用自动完成注册，调用方无需关心有哪些实现。

### cadvisor.New 构造流程

入口在 kubelet `run()`（`cmd/kubelet/app/server.go`）：

```go
// cmd/kubelet/app/server.go（简化）
if kubeDeps.CAdvisorInterface == nil {
    imageFsInfoProvider := cadvisor.NewImageFsInfoProvider(s.ContainerRuntime, s.RemoteRuntimeEndpoint)
    kubeDeps.CAdvisorInterface, err = cadvisor.New(imageFsInfoProvider, s.RootDirectory, ...)
}
```

`cadvisor.New`（cadvisor_linux.go:82）内部调用 `manager.New`（manager.go:149），后者：

1. 初始化 `init()` 中设置的 `flags`（defaultHousekeepingInterval、disableRootCgroupStats 等），对每个 flag 设置默认值
2. 收集注册的指标大类（`includedMetrics`），如 `cadvisormetrics.CpuUsageMetrics`、`cadvisormetrics.MemoryUsageMetrics` 等
3. 获取 `fsInfo` — 调用 `fs.NewFsInfo(context)` 获取挂载点信息
4. 获取 `machineInfo` — 调用 `machine.Info(sysFs, fsInfo, inHostNamespace)` 获取硬件信息
5. 构造并返回 `&manager{containers, memoryCache, fsInfo, sysFs, ...}`

### kubelet 调用 MachineInfo

```go
// pkg/kubelet/kubelet.go
machineInfo, err := klet.cadvisor.MachineInfo()
```

底层调用 `cadvisorManager.GetMachineInfo()`（manager.go:813），通过 `m.machineInfo.Clone()` 返回深拷贝——Clone 手动复制 MemoryByType、DiskMap 等 map 字段以避免并发写冲突。

### cadvisor.Start 启动流程

kubelet 在 `initializeRuntimeDependentModules()`（kubelet.go:1371）中启动 cadvisor：

```go
// pkg/kubelet/kubelet.go:1372
if err := kl.cadvisor.Start(); err != nil {
    klog.ErrorS(err, "Failed to start cAdvisor")
    os.Exit(1)  // 启动失败直接退出，由 babysitter 重启 kubelet
}
```

`cadvisorClient.Start()`（cadvisor_linux.go:128）直接透传给 `cc.Manager.Start()`（manager.go:271）。

`manager.Start` 的核心工作：

**① 通过 InitializePlugins 获取 containerWatchers**

```go
// vendor/github.com/google/cadvisor/container/factory.go:157
func InitializePlugins(factory info.MachineInfoFactory, fsInfo fs.FsInfo, includedMetrics MetricSet) []watcher.ContainerWatcher {
    pluginsLock.Lock()
    defer pluginsLock.Unlock()
    containerWatchers := []watcher.ContainerWatcher{}
    for name, plugin := range plugins {
        watcher, err := plugin.Register(factory, fsInfo, includedMetrics)
        // ...
        if watcher != nil {
            containerWatchers = append(containerWatchers, watcher)
        }
    }
    return containerWatchers
}
```

**② 额外注册 rawWatcher**（cgroup 原始目录监听，不走 plugin 机制）：

```go
err = raw.Register(m, m.fsInfo, m.includedMetrics, m.rawContainerCgroupPathPrefixWhiteList)
rawWatcher, err := raw.NewRawContainerWatcher()
m.containerWatchers = append(m.containerWatchers, rawWatcher)
```

**③ 启动四条 goroutine**：

```go
go m.watchForNewContainers(quitWatcher)    // inotify 监听 cgroup 目录变化
go m.watchForNewOoms()                     // 解析 /dev/kmsg 检测 OOM
go m.globalHousekeeping(quitGlobalHousekeeping)  // 定期刷新容器列表
go m.updateMachineInfo(quitUpdateMachineInfo)    // 定时刷新机器信息
```

### watchForNewContainers：生产者-消费者模型

**生产者侧 — rawContainerWatcher.Start**（raw/watcher.go:71）：

遍历 cgroupPaths 顶级目录，对每个目录调用 `watchDirectory`：

```go
for _, cgroupPath := range w.cgroupPaths {
    _, err := w.watchDirectory(events, cgroupPath, "/")
    // ...
}
```

`watchDirectory` 读取初始子目录（已存在的容器）产生 ContainerEvent 发往 events chan，同时设置 inotify 监听。

外部 goroutine 的 `processEvent`（raw/watcher.go:179）处理内核发来的 inotify 事件，将 inotify.InCreate/InMovedTo 映射为 `watcher.ContainerAdd`，InDelete/InMovedFrom 映射为 `watcher.ContainerDelete`，然后：

- `ContainerAdd` → `watchDirectory`（递归监听子目录）
- `ContainerDelete` → `RemoveWatch`（取消监听）

**消费者侧 — watchForNewContainers**（manager.go:1139）：

首先遍历所有 containerWatchers 调用其 Start 启动生产者：

```go
watched := make([]watcher.ContainerWatcher, 0)
for _, watcher := range m.containerWatchers {
    err := watcher.Start(m.eventsChannel)
    // ...
    watched = append(watched, watcher)
}
```

然后在 goroutine 中消费 `m.eventsChannel`：

```go
go func() {
    for {
        select {
        case event := <-m.eventsChannel:
            switch {
            case event.EventType == watcher.ContainerAdd:
                err = m.createContainer(event.Name, event.WatchSource)
            case event.EventType == watcher.ContainerDelete:
                err = m.destroyContainer(event.Name)
            }
        }
    }
}()
```

`createContainer` 内部为新容器启动独立的数据采集 goroutine，周期性读取 `/proc/<pid>/` 下的统计文件。

### watchForNewOoms：OOM 检测

```go
// vendor/github.com/google/cadvisor/manager/manager.go:1202
func (m *manager) watchForNewOoms() error {
    outStream := make(chan *oomparser.OomInstance, 10)
    oomLog, err := oomparser.New()
    go oomLog.StreamOoms(outStream)  // 生产者：解析 /dev/kmsg
    go func() {
        for oomInstance := range outStream {  // 消费者
            // 发布 EventOom（容器发生 OOM）
            newEvent := &info.Event{ContainerName: oomInstance.ContainerName, EventType: info.EventOom}
            m.eventHandler.AddEvent(newEvent)
            // 发布 EventOomKill（含 Pid/ProcessName）
            newEvent = &info.Event{
                ContainerName: oomInstance.VictimContainerName,
                EventType:     info.EventOomKill,
                EventData:     info.EventData{OomKill: &info.OomKillEventData{Pid: oomInstance.Pid, ...}},
            }
            m.eventHandler.AddEvent(newEvent)
        }
    }()
}
```

`StreamOoms`（oomparser.go:117）：逐行扫描 /dev/kmsg，`checkIfStartOfOomMessages` 判断是否为 OOM 开始行，随后用 `getContainerName` 的正则匹配容器进程名，`getProcessNamePid` 提取 Pid，构造 OomInstance 发往 outStream。

### globalHousekeeping：定期刷新容器列表

```go
// vendor/github.com/google/cadvisor/manager/manager.go:376
func (m *manager) globalHousekeeping(quit chan error) {
    longHousekeeping := 100 * time.Millisecond
    ticker := time.NewTicker(*globalHousekeepingInterval)
    for {
        select {
        case t := <-ticker.C:
            start := time.Now()
            err := m.detectSubcontainers("/")  // 探测子容器目录，发现新增/删除
            duration := time.Since(start)
            if duration >= longHousekeeping {
                klog.V(3).Infof("Global Housekeeping(%d) took %s", t.Unix(), duration)
            }
        case <-quit:
            return
        }
    }
}
```

`detectSubcontainers` 主动扫描 cgroup 目录树，补充 inotify 可能遗漏的容器（如 kubelet 重启时已存在的容器）。

### updateMachineInfo：定时刷新机器信息

```go
// vendor/github.com/google/cadvisor/manager/manager.go:354
func (m *manager) updateMachineInfo(quit chan error) {
    ticker := time.NewTicker(*updateMachineInfoInterval)
    for {
        select {
        case <-ticker.C:
            info, err := machine.Info(m.sysFs, m.fsInfo, m.inHostNamespace)
            m.machineMu.Lock()
            m.machineInfo = *info
            m.machineMu.Unlock()
        case <-quit:
            return
        }
    }
}
```

kubelet 查询机器信息时（`GetMachineInfo`）通过 `m.machineMu.RLock()` 保护并发读。
