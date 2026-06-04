# 第15章 创建 init 和 app 容器的后期工作

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 15 章 — 创建 init 和 app 容器的后期工作
> **源码入口**: `pkg/kubelet/kuberuntime/kuberuntime_container.go`

---

## 核心机制一览

1. **InternalContainerLifecycle 是资源管理器与容器生命周期的桥梁**：四个钩子（PreCreateContainer / PreStartContainer / PreStopContainer / PostStopContainer）将 cpuManager、memoryManager、topologyManager 与容器创建/销毁解耦；PreCreateContainer 设置 cpuset/numa，PreStartContainer 把 containerID 注册进三个 manager 的本地状态。

2. **CreateContainer 在指定 sandbox 内创建容器**：入参 `podSandboxID` 决定新容器加入哪个 Pod 命名空间；底层是 `runtimeService.CreateContainer` gRPC，返回 containerID，失败时产生 `FailedToCreateContainer` event。

3. **StartContainer 无返回值，靠 error 判断成功**：`runtimeService.StartContainer(containerID)` 是纯命令式 gRPC，没有响应体；成功则产生 `StartedContainer` event 并创建 symlink 日志文件。

4. **PostStart hook 的三种类型及优先级**：`runner.Run` 用 switch-case 控制执行顺序，优先 Exec（在容器内执行命令）> HTTPGet（发 HTTP 请求到容器）> TCPSocket（v1.21 仍不支持）。

5. **HTTPGet hook 的 host 回退机制**：若 handler 未配置 host，回退到 `containerManager.GetPodStatus` 获取 Pod 第一个 IP；端口未配置则默认 80；body 最多读取 10k。

6. **app 容器与 init 容器共用 `startContainer`**：SyncPod Step 7 遍历 `podContainerChanges.ContainersToStart`，对每个 app 容器调用同一个 `start` 函数（内部调用 `startContainer`），与 init 容器路径完全一致。

---

## 全章调用链总图

```
startContainer (kuberuntime_container.go:136)
  │
  ├─ Step 1: pull image          ← 第14章
  │
  ├─ Step 2: generateContainerConfig  ← 第14章
  │
  ├─ PreCreateContainer (internal_container_lifecycle.go:29)
  │    cpuManager.SetCPUs → 设置 cpuset
  │    memoryManager → 设置 numa 亲和
  │
  ├─ CreateContainer gRPC (remote_runtime.go:217)
  │    runtimeService.CreateContainer(podSandboxID, containerConfig, podSandboxConfig)
  │    → containerID
  │    失败 → event: FailedToCreateContainer
  │
  ├─ PreStartContainer (internal_container_lifecycle.go:43)
  │    cpuManager.AddContainer(containerID)
  │    memoryManager.AddContainer(containerID)
  │    topologyManager.AddContainer(containerID)
  │
  ├─ StartContainer gRPC (remote_runtime.go:244)
  │    runtimeService.StartContainer(containerID)
  │    失败 → event: FailedToStartContainer
  │    成功 → event: StartedContainer
  │         symlink: legacyLogDir → containerLogsPath
  │
  └─ Step 4: PostStart lifecycle hook
       runner.Run (lifecycle/handlers.go:59)
         ├─ Exec   → m.runner.RunInContainer(containerID, cmd, timeout)
         │           → ExecSync gRPC (remote_runtime.go)
         ├─ HTTPGet → runHTTPHandler (handlers.go:108)
         │            host 回退: handler.Host → PodStatus.IPs[0]
         │            port 回退: handler.Port → 80
         │            url = "http://host:port/path"
         │            getHTTPRespBody (最多读 10k)
         └─ TCPSocket → 暂不支持（v1.21）

SyncPod Step 7: app 容器 (kuberuntime_manager.go:702)
  for _, idx := range podContainerChanges.ContainersToStart {
      start("container", ..., &pod.Spec.Containers[idx])
  }
  └─ 同样调用 startContainer，路径与 init 容器完全一致
```

---

## §01 init 容器步骤 2 的剩余工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 容器生命周期钩子接口 | [cm/internal_container_lifecycle.go](kubernetes/pkg/kubelet/cm/internal_container_lifecycle.go) | `InternalContainerLifecycle:29` |
| PreCreateContainer（Linux） | [cm/internal_container_lifecycle_linux.go](kubernetes/pkg/kubelet/cm/internal_container_lifecycle_linux.go) | `PreCreateContainer:29` |
| PreStartContainer | [cm/internal_container_lifecycle.go](kubernetes/pkg/kubelet/cm/internal_container_lifecycle.go) | `PreStartContainer:43` |
| CreateContainer gRPC | [cri/remote/remote_runtime.go](kubernetes/pkg/kubelet/cri/remote/remote_runtime.go) | `CreateContainer:217` |

### Pod 容器生命周期总览

```
pod 生命周期时序（横轴为时间）：

  0          3        5   6   7        11  12  13
  │          │        │   │   │         │   │   │
  ├──────────┤        │   │   │         │   │   │
  init C              │   │   │         │   │   │
                      ├───┴───┴─────────┴───┤   │
                      main container            │
                          ├─────────────────────┤
                          postStart hook    prestop hook
                              │─────────────────────────
                              liveness probe
                                  │──────────────────────
                                  readiness probe
  ───────────────────────────────────────────────────────
  Pause 容器（整个 Pod 生命周期内存在，提供网络命名空间）
```

init 容器（串行）执行完毕后，main container 才启动并触发 postStart hook；liveness/readiness probe 在 main container 启动后才开始探测。

### InternalContainerLifecycle：资源管理器的钩子接口

```go
// pkg/kubelet/cm/internal_container_lifecycle.go:29
type InternalContainerLifecycle interface {
    PreCreateContainer(pod *v1.Pod, container *v1.Container,
        containerConfig *runtimeapi.ContainerConfig) error
    PreStartContainer(pod *v1.Pod, container *v1.Container, containerID string) error
    PreStopContainer(containerID string) error
    PostStopContainer(containerID string) error
}
```

这四个钩子将 cpuManager / memoryManager / topologyManager 的资源分配逻辑嵌入容器生命周期，而不需要 `startContainer` 直接感知资源管理器的存在——这是一层解耦。

**PreCreateContainer**（在 `generateContainerConfig` 之后、`CreateContainer` 之前调用）：

```go
// pkg/kubelet/cm/internal_container_lifecycle_linux.go:29
func (i *internalContainerLifecycleImpl) PreCreateContainer(...) error {
    if i.cpuManager != nil {
        // 计算该容器应绑定的 CPU，写入 containerConfig.Linux.Resources.CpusetCpus
        allocatedCPUs := i.cpuManager.GetCPUSetOrDefault(pod.UID, container.Name)
        containerConfig.Linux.Resources.CpusetCpus = allocatedCPUs.String()
    }
    if i.memoryManager != nil {
        // 获取 numa node 亲和列表，写入 CpusetMems
        numaNodes := i.memoryManager.GetMemoryNUMANodes(pod, container)
        containerConfig.Linux.Resources.CpusetMems = strings.Join(affinity, ",")
    }
    return nil
}
```

**PreStartContainer**（在 `StartContainer` 之前调用）：

```go
// pkg/kubelet/cm/internal_container_lifecycle.go:43
func (i *internalContainerLifecycleImpl) PreStartContainer(...) error {
    if i.cpuManager != nil {
        i.cpuManager.AddContainer(pod, container, containerID)
    }
    if i.memoryManager != nil {
        i.memoryManager.AddContainer(pod, container, containerID)
    }
    if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.TopologyManager) {
        i.topologyManager.AddContainer(pod, containerID)
    }
    return nil
}
```

PreCreateContainer 写资源配置到 ContainerConfig（影响 cgroup 设置），PreStartContainer 在 containerID 确定后把它注册进各 manager 的内存映射（containerID → pod/container），供后续查询和清理使用。

### CreateContainer：在 sandbox 内创建容器

```go
// pkg/kubelet/cri/remote/remote_runtime.go:217
func (r *remoteRuntimeService) CreateContainer(
    podSandBoxID string,
    config *runtimeapi.ContainerConfig,
    sandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {

    resp, err := r.runtimeClient.CreateContainer(ctx, &runtimeapi.CreateContainerRequest{
        PodSandboxId:  podSandBoxID,
        Config:        config,
        SandboxConfig: sandboxConfig,
    })
    // ...
    return resp.ContainerId, nil
}
```

三个入参的含义：
- `podSandBoxID`：之前创建的 sandbox 容器 ID，新容器将加入该 sandbox 的 net/pid/ipc 命名空间
- `containerConfig`：本容器的完整配置（命令、环境变量、挂载、cgroup 参数等）
- `sandboxConfig`：sandbox 的配置信息（DNS、hostname、端口映射等），CRI shim 可能需要引用

创建失败时在 `startContainer` 中产生 `FailedToCreateContainer` event，返回 `ErrCreateContainer` 错误码。

---

## §02 创建 init 容器步骤 3、4——启动容器与 postStart hook

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| StartContainer gRPC | [cri/remote/remote_runtime.go](kubernetes/pkg/kubelet/cri/remote/remote_runtime.go) | `StartContainer:244` |
| postStart hook 入口 | [lifecycle/handlers.go](kubernetes/pkg/kubelet/lifecycle/handlers.go) | `Run:59` |
| HTTPGet hook | [lifecycle/handlers.go](kubernetes/pkg/kubelet/lifecycle/handlers.go) | `runHTTPHandler:108` |

### Step 3：StartContainer

`StartContainer` 仅接受 containerID，无其他参数，也无响应体——执行结果完全由 error 判断：

```go
// pkg/kubelet/cri/remote/remote_runtime.go:244
func (r *remoteRuntimeService) StartContainer(containerID string) error {
    _, err := r.runtimeClient.StartContainer(ctx, &runtimeapi.StartContainerRequest{
        ContainerId: containerID,
    })
    // 无响应体，return nil 即成功
    return err
}
```

StartContainer 成功后，`startContainer` 做两件事：
1. 产生 `StartedContainer` event（Normal 类型，记录到 API Server）
2. 创建 symlink：`legacyLogSymlink → containerLogsPath`，让旧版日志路径也能访问当前日志

### Step 4：PostStart lifecycle hook

PostStart 是在容器进程启动后**异步**执行的钩子，kubelet 不等待其完成（与容器主进程并行）。实际执行入口是 `runner.Run`：

```go
// pkg/kubelet/lifecycle/handlers.go:59
func (hr *handlerRunner) Run(containerID, pod, container, handler) (string, error) {
    switch {
    case handler.Exec != nil:
        // 优先：在容器内执行命令
        return hr.runInContainer(containerID, handler.Exec.Command, timeout)
    case handler.HTTPGet != nil:
        // 其次：发 HTTP GET 请求
        return hr.runHTTPHandler(pod, container, handler)
    default:
        // TCPSocket：v1.21 暂不支持
        return "", fmt.Errorf("invalid handler: %v", handler)
    }
}
```

**handler.Exec**：通过 `m.runner.RunInContainer` 在容器内执行命令，底层走 `ExecSync` gRPC（同步执行，有超时控制；`DeadlineExceeded` 错误被包装为 timeout probe error）。

**handler.HTTPGet**：

```go
// pkg/kubelet/lifecycle/handlers.go:108
func (hr *handlerRunner) runHTTPHandler(pod, container, handler) (string, error) {
    // host 回退逻辑
    host := handler.HTTPGet.Host
    if len(host) == 0 {
        status, _ := hr.containerManager.GetPodStatus(pod.UID, pod.Name, pod.Namespace)
        host = status.IPs[0]   // 使用 Pod 第一个 IP
    }
    // port 回退逻辑
    port := resolvePort(handler.HTTPGet.Port, container)  // 未配置则默认 80
    url := fmt.Sprintf("http://%s/%s", net.JoinHostPort(host, port), handler.HTTPGet.Path)

    resp, err := hr.httpGetter.Get(url)
    return getHTTPRespBody(resp, err)  // 最多读取 10k body
}
```

host 回退的原因：PostStart hook 通常在 Pod 内部（同 namespace）调用自身服务，不需要外部 IP；但 handler 未指定 host 时，kubelet 用 Pod IP 代替。

### app 容器与 init 容器共用同一 startContainer

SyncPod Step 7 处理业务容器，代码路径与 Step 6（init 容器）完全相同：

```go
// pkg/kubelet/kuberuntime/kuberuntime_manager.go（Step 7）
for _, idx := range podContainerChanges.ContainersToStart {
    start("container", metrics.Container, containerStartSpec(&pod.Spec.Containers[idx]))
}
```

`start` 内部调用 `startContainer`，四步流程（pull image → create → start → postStart hook）对 init 容器和业务容器没有任何区别，区别只在 SyncPod 的调度逻辑上（init 容器必须串行完成，app 容器并行启动）。
