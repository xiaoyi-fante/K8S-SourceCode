# 第13章 kubelet containerRuntime 与 sandbox 容器

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 13 章 — containerRuntime 接口与 sandbox 创建
> **源码入口**: `pkg/kubelet/kuberuntime/kuberuntime_manager.go`、`pkg/kubelet/kuberuntime/kuberuntime_sandbox.go`

---

## 核心机制一览

1. **CRI 将 kubelet 与容器运行时解耦**：kubelet 通过 gRPC 调用 CRI Shim，CRI Shim 再调用具体的 Container Engine（containerd、kata 等）和 CNI 插件，三层分离让 kubelet 无需关心底层实现细节。

2. **容器运行时的三类接口**：kubelet 内部 `kubecontainer.Runtime`（管理容器生命周期）、`kubecontainer.StreamingRuntime`（Exec/Attach/PortForward 的 URL 重定向）、`kubecontainer.CommandRunner`（在容器内同步执行命令），三者组合成 `KubeGenericRuntime`，由 `kubeGenericRuntimeManager` 统一实现。

3. **`kubeGenericRuntimeManager` 是 kubelet 的 runtime 门面**：持有 `runtimeService`（gRPC 容器管理）和 `imageService`（gRPC 镜像管理）两个 remote client，所有 CRI 调用都经过它。kubelet 启动后将同一个 `runtime` 对象分别赋给 `containerRuntime`、`streamingRuntime`、`runner` 三个字段。

4. **Sandbox（pause 容器）是 Pod 网络和命名空间的锚**：sandbox 先于所有业务容器创建，为 Pod 建立独立的 net/pid/ipc namespace；后续 container 只需 join 已有 sandbox 的 namespace。K8s 默认 sandbox 就是 `gcr.io/google_containers/pause-amd64` 镜像。

5. **createPodSandbox 全流程**：SyncPod Step 4 → `generatePodSandboxConfig`（组装 DNS、Hostname、端口映射、SecurityContext、LinuxSandboxConfig）→ `osInterface.MkdirAll`（创建 Pod 日志目录）→ `runtimeService.RunPodSandbox`（gRPC 调用 CRI Shim）→ 校验返回的 PodSandboxId 非空。

6. **sandbox 创建错误的两条路径**：若创建 sandbox 期间 Pod 已被删除（`podIsTerminated`），则静默 return，无需产生 Event；其余错误一律记录 `FailedCreatePodSandBox` Event，并终止 SyncPod 流程（基础容器未就绪，继续无意义）。

---

## 全章调用链总图

```
kubelet.New()
  │
  └─── kuberuntime.NewKubeGenericRuntimeManager(...)    kuberuntime_manager.go:160
          │  runtimeService  = internalapi.RuntimeService  (gRPC client)
          │  imageService    = internalapi.ImageManagerService (gRPC client)
          │
          └─── 返回 kubeGenericRuntimeManager
                  klet.containerRuntime  = runtime
                  klet.streamingRuntime  = runtime
                  klet.runner            = runtime

kubelet SyncPod（Step 4）
  │
  └─── m.createPodSandbox(pod, attempt)      kuberuntime_sandbox.go:37
          │
          ├─── m.generatePodSandboxConfig(pod, attempt)   :76
          │       ├─── m.runtimeHelper.GetPodDNS(pod)     dns.go:332
          │       │       ├─── getPodDNSType(pod)          dns.go:270
          │       │       └─── generateSearchesForDNSClusterFirst()  dns.go:147
          │       ├─── GeneratePodHostNameAndDomain()
          │       ├─── 端口映射 → runtimeapi.PortMapping
          │       └─── LinuxPodSandboxConfig（SecurityContext、cgroup）
          │
          ├─── osInterface.MkdirAll(logDir, 0755)
          │       logDir = /var/log/pods/<ns>/<pod_name>/<pod_id>
          │
          ├─── m.runtimeService.RunPodSandbox(config, runtimeHandler)
          │       remote_runtime.go:101
          │       └─── r.runtimeClient.RunPodSandbox(ctx, &RunPodSandboxRequest{...})
          │               → gRPC → CRI Shim → Container Engine
          │
          └─── 校验 resp.PodSandboxId != ""
                  空 → FailedCreatePodSandBox Event
                  非空 → 返回 podSandboxID
```

---

## §01 containerRuntime 原理简介

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| CRI RuntimeService gRPC client | [remote_runtime.go](kubernetes/pkg/kubelet/cri/remote/remote_runtime.go) | `RunPodSandbox:101` |
| kubecontainer.Runtime 接口 | [runtime.go](kubernetes/pkg/kubelet/container/runtime.go) | `Runtime:67` |
| kubecontainer.StreamingRuntime | [runtime.go](kubernetes/pkg/kubelet/container/runtime.go) | `StreamingRuntime:128` |
| kubecontainer.CommandRunner | [runtime.go](kubernetes/pkg/kubelet/container/runtime.go) | `CommandRunner:156` |

### kubelet 架构中的 runtime 层

```
Kubelet
  ├─── Kubelet Server        ├─── Container Manager    ├─── Volume Manager
  ├─── Eviction              ├─── cAdvisor             ├─── Metrics and stats
  ├─── Generic Runtime Manager
  │       └─── Container Runtime Interface (CRI)
  │               ├─── dockershim (内置，支持 Docker)
  │               └─── remote   (外部 CRI Shim)
  └─── Network Plugin  └─── Docker

                              CRI Container Runtime
                                ├─── CRI Server (shim)
                                ├─── Streaming Server
                                ├─── CNI    ├─── Containers  ├─── Images
                                └─── Container Engine
                                        ├─── runc  ├─── containerd  └─── kata
```

kubelet 通过 CRI 接口与外部容器运行时交互，两种实现：
- **dockershim**：内置，kubelet 直接调用 Docker daemon（v1.11+ 内置了 dockershim，CNI 由 dockershim 负责）
- **remote**：外部 CRI Shim，支持 containerd、cri-o、kata 等；kubelet 通过 unix socket 连接

### 容器运行时的演进三阶段

| 阶段 | K8s 版本 | 描述 |
|------|---------|------|
| 第一阶段 | < v1.5 | kubelet 内置 Docker + rkt 支持，直接调用；CNI 独立插件 |
| 第二阶段 | v1.5 | 引入 CRI 接口（gRPC Protocol Buffer）；dockershim 作为内置 shim 过渡；Docker 运行时后来通过 dockershim 接入 |
| 第三阶段 | v1.11+ | kubelet 内置 dockershim 代码，CNI 由 dockershim 负责；同时支持外部 CRI Shim |

### CRI 接口分类

CRI 包含三类接口，运行时需全部实现：

**ImageService（5 个接口）**：`PullImage`、`ListImages`、`RemoveImage`、`ImageStatus`、`ImageFsInfo`

**RuntimeService（分四组）**：

| 接口组 | 作用 |
|-------|------|
| PodSandbox 管理 | `RunPodSandbox` / `StopPodSandbox` / `RemovePodSandbox` / `PodSandboxStatus` / `ListPodSandbox` |
| Container 管理 | `CreateContainer` / `StartContainer` / `StopContainer` / `RemoveContainer` / `ListContainers` / `ContainerStatus` |
| Streaming API | `ExecSync` / `Exec` / `Attach` / `PortForward`（返回 URL，由 Streaming Server 处理实际流量） |
| 状态接口 | `Version` / `Status` |

**Streaming API 的设计意图**：Exec/Attach/PortForward 流量若都经过 kubelet，会成为节点网络瓶颈。因此 CRI 要求运行时启动一个独立 Streaming Server，CRI 接口只返回该 Server 的 URL，kubelet 再将 URL 转发给 Kubernetes API Server，由 API Server 与 Streaming Server 直接建连，绕过 kubelet。

---

## §02 kubelet containerRuntime 接口定义和初始化

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| KubeGenericRuntime 接口 | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `KubeGenericRuntime:147` |
| kubeGenericRuntimeManager 结构体 | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `kubeGenericRuntimeManager:83` |
| NewKubeGenericRuntimeManager | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `NewKubeGenericRuntimeManager:160` |

### KubeGenericRuntime 接口

```go
// pkg/kubelet/kuberuntime/kuberuntime_manager.go:147
type KubeGenericRuntime interface {
    kubecontainer.Runtime
    kubecontainer.StreamingRuntime
    kubecontainer.CommandRunner
}
```

三个子接口的职责：

**`kubecontainer.Runtime`**（runtime.go:67）：容器生命周期管理的核心接口，包含 `SyncPod`、`KillPod`、`GetPods`、`GetPodStatus`、`GarbageCollect` 等。

**`kubecontainer.StreamingRuntime`**（runtime.go:128）：streaming 调用（exec/attach/port-forward），返回 URL 而非直接执行，让 Streaming Server 承接实际流量：
```go
type StreamingRuntime interface {
    GetExec(id ContainerID, cmd []string, stdin, stdout, stderr, tty bool) (*url.URL, error)
    GetAttach(id ContainerID, stdin, stdout, stderr, tty bool) (*url.URL, error)
    GetPortForward(podName, podNamespace string, podUID types.UID, ports []int32) (*url.URL, error)
}
```

**`kubecontainer.CommandRunner`**（runtime.go:156）：在容器内同步执行命令，超时时间为 timeout：
```go
type CommandRunner interface {
    RunInContainer(id ContainerID, cmd []string, timeout time.Duration) ([]byte, error)
}
```

### kubeGenericRuntimeManager 关键字段

```go
// pkg/kubelet/kuberuntime/kuberuntime_manager.go:83
type kubeGenericRuntimeManager struct {
    runtimeName   string
    osInterface   kubecontainer.OSInterface         // 节点 OS 操作（文件、目录）
    runtimeHelper kubecontainer.RuntimeHelper       // 辅助方法（获取 DNS、Hostname 等）

    // gRPC service clients — 所有 CRI 调用的出口
    runtimeService internalapi.RuntimeService
    imageService   internalapi.ImageManagerService

    imagePuller        images.ImageManager
    livenessManager    proberesults.Manager
    readinessManager   proberesults.Manager
    startupManager     proberesults.Manager

    internalLifecycle  cm.InternalContainerLifecycle   // 容器生命周期钩子（cpuMgr/memMgr）
    // ...
}
```

`runtimeService` 和 `imageService` 是最关键的两个字段——所有 CRI 调用都经由这两个 gRPC client 发出。

### NewKubeGenericRuntimeManager 初始化

```
NewKubeGenericRuntimeManager(...)    kuberuntime_manager.go:160
  │
  ├─── 填充所有基础字段（recorder、osInterface、machineInfo 等）
  │
  ├─── 获取 runtime 版本
  │       typedVersion, err := kubeRuntimeManager.getTypedVersion()
  │
  ├─── 校验 kubeRuntimeAPIVersion（目前只支持当前固定版本）
  │
  ├─── kubeRuntimeManager.runtimeName = typedVersion.RuntimeName
  │       klog.InfoS("Container runtime initialized", "containerRuntime", ...)
  │
  ├─── 确保 pod log 目录存在
  │       osInterface.MkdirAll(podLogsRootDirectory, 0755)
  │
  ├─── 配置镜像凭证提供程序（imageCredentialProviderConfigFile）
  │
  ├─── 初始化 imagePuller（images.NewImageManager）
  │
  └─── 初始化 runner（lifecycle.NewHandlerRunner）
          podStateProvider = podStateProvider
```

### runtime 对象赋值给 kubelet 三个字段

```go
// kubelet.go（NewMainKubelet 内）
klet.containerRuntime  = runtime
klet.streamingRuntime  = runtime
klet.runner            = runtime
```

同一个 `kubeGenericRuntimeManager` 实例同时满足三个接口，通过不同字段以不同职责被调用。

---

## §03 sandbox 简介和 PodSandbox

### kubelet 创建 Pod 的大致过程

```
kubelet.SyncPod()    kuberuntime_manager.go:702
  │
  ├─── Step 1: 计算 sandbox 和容器的变更
  ├─── Step 2: 如果 sandbox 已变更则 Kill pod
  ├─── Step 3: Kill 不再需要的运行中容器
  ├─── Step 4: 创建 sandbox（若不存在）
  │       └─── m.createPodSandbox(pod, attempt)
  ├─── Step 5: 启动 ephemeral containers
  ├─── Step 6: 启动 init container
  └─── Step 7: 启动业务容器
```

kubelet 通过 gRPC 调用 CRI 接口 → 先创建 PodSandbox → sandbox 就绪后，继续调用 Image/Container 接口拉镜像和创建容器，shim 将这些请求翻译为具体 runtime API。

### 什么是 Sandbox

Sandbox 和 VM 都属于虚拟化技术：
- 两者都用 cgroups 做资源配额，从概念上都抽离出一个隔离的运行环境，区别仅在于资源隔离的实现
- **对于 kata**：PodSandbox 就是虚拟机
- **对于 docker/containerd**：PodSandbox 就是 Linux namespace

**Sandbox 为 Pod 中的容器提供网络环境**：

```
                        Pod
/proc/{pid}/ns/net -> net:[4026532483]

  Container A                Container B
       │                          │
       └────── Infra container ───┘
               (pause 容器, -net=k8s.gcr.io/pause)
```

同一个 Pod 中的多个容器共同分配到同一个 Host 上并共享网络栈，可以通过 localhost 互相访问端口和服务。这个共享网络设备由 Pod Sandbox 中的 pause 容器在 `RunPodSandbox` 方法中启动时创建。

### K8s 中的 PodSandbox

- Sandbox 是 K8s 为兼容不同运行时环境预留的空间（抽象层），K8s 允许 low-level runtime 依据不同实现去创建不同的 PodSandbox
- sandbox 创建后，Kubelet 可以在里面创建用户容器；删除 Pod 时，先移除 Sandbox，再停止里面的所有容器
- 对 container 来说，当 Sandbox 运行后，只需要将新的 container 的 namespace 加入到已有的 sandbox namespace 中

**K8s 默认的 Pod Sandbox**：在默认情况下，CRI 体系里，Pod Sandbox 其实就是 **pause 容器**。Kubelet 代码引用的 `defaultSandboxImage` 其实就是官方提供的 `gcr.io/google_containers/pause-amd64` 镜像。

---

## §04 containerRuntime 创建 sandbox 源码阅读

### 读码入口表

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| createPodSandbox | [kuberuntime_sandbox.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_sandbox.go) | `createPodSandbox:37` |
| generatePodSandboxConfig | [kuberuntime_sandbox.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_sandbox.go) | `generatePodSandboxConfig:76` |
| GetPodDNS | [dns.go](kubernetes/pkg/kubelet/network/dns/dns.go) | `GetPodDNS:332` |
| getPodDNSType | [dns.go](kubernetes/pkg/kubelet/network/dns/dns.go) | `getPodDNSType:270` |
| generateSearchesForDNSClusterFirst | [dns.go](kubernetes/pkg/kubelet/network/dns/dns.go) | `generateSearchesForDNSClusterFirst:147` |
| RunPodSandbox（remote） | [remote_runtime.go](kubernetes/pkg/kubelet/cri/remote/remote_runtime.go) | `RunPodSandbox:101` |
| SyncPod | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `SyncPod:702` |

### createPodSandbox 流程

```
createPodSandbox(pod, attempt)    kuberuntime_sandbox.go:37
  │
  ├─── ① generatePodSandboxConfig(pod, attempt)   :76
  │       │
  │       ├─── metadata: name / namespace / uid / attempt
  │       │
  │       ├─── m.runtimeHelper.GetPodDNS(pod)      dns.go:332
  │       │       ├─── getPodDNSType(pod)            → ClusterFirst / Default / None / ClusterFirstWithHostNet
  │       │       │       判断依据：pod.Spec.DNSPolicy
  │       │       └─── 根据 dnsType 生成 DNSConfig
  │       │               ClusterFirst:
  │       │                 dnsConfig.Servers = clusterDNS 地址列表
  │       │                 dnsConfig.Searches = generateSearchesForDNSClusterFirst()
  │       │                     nsSvcDomain  = "<ns>.svc.<ClusterDomain>"
  │       │                     svcDomain    = "svc.<ClusterDomain>"
  │       │                     return [nsSvcDomain, svcDomain, ClusterDomain, ...hostSearch]
  │       │               ClusterFirstWithHostNet: 额外把节点 DNS 也加进去
  │       │               podDNSHost: 读取节点 /etc/resolv.conf
  │       │
  │       ├─── 非 hostNetwork Pod → GeneratePodHostNameAndDomain()
  │       │       podHostname = pod.Spec.Hostname（默认 pod.Name）
  │       │       podSandboxConfig.Hostname = podHostname
  │       │
  │       ├─── 端口映射 → []runtimeapi.PortMapping
  │       │       for c in pod.Spec.Containers:
  │       │           for port in c.Ports:
  │       │               HostIp, HostPort, ContainerPort, Protocol
  │       │
  │       └─── LinuxPodSandboxConfig
  │               SecurityContext（cgroupParent、privileged、sysctls 等）
  │               （PodSandboxConfig 是发往 remote runtime 的 pb 接口字段，
  │                 位于 vendor/k8s.io/cri-api/pkg/apis/runtime/v1alpha2/api.pb.go）
  │
  ├─── ② osInterface.MkdirAll(logDir, 0755)
  │       logDir = BuildPodLogsDirectory(pod.Namespace, pod.Name, pod.UID)
  │                → /var/log/pods/<ns>/<pod_name>/<pod_id>
  │
  ├─── ③ m.runtimeService.RunPodSandbox(podSandboxConfig, runtimeHandler)
  │       remote_runtime.go:101
  │       └─── r.runtimeClient.RunPodSandbox(ctx, &runtimeapi.RunPodSandboxRequest{
  │                Config:         config,
  │                RuntimeHandler: runtimeHandler,
  │            })
  │            → gRPC → CRI Shim → Container Engine（containerd/kata/...）
  │            可能失败原因：网络问题 or runtime 问题
  │
  └─── ④ 校验结果
          if resp.PodSandboxId == "" → FailedCreatePodSandBox Event, return err
          非空 → return podSandboxID（后续创建容器使用）
```

### SyncPod 中判断 createPodSandbox 的 err

```
createPodSandboxResult, err := m.createPodSandbox(pod, podContainerChanges.Attempt)
  │
  ├─── 如果创建 pod 过程中又删除了 pod（podIsTerminated）
  │       // createPodSandbox 可能返回 CNI、CRI 等错误
  │       // 如果 pod 已经被删除了，那么这个错误不重要
  │       return   ← 静默返回，无须产生 Event
  │
  └─── 其余错误
          metrics.StartedPodsErrorsTotal.Inc()
          createSandboxResult.Fail(kubecontainer.ErrCreatePodSandbox, msg)
          m.recorder.Eventf(ref, v1.EventTypeWarning,
              events.FailedCreatePodSandBox, ...)
          return   ← 终止 SyncPod，等待下次重试
```

**为什么 sandbox 创建失败就终止整个 SyncPod**：sandbox 是 Pod 的网络和 namespace 锚，业务容器需要 join 已有 sandbox 的 namespace 才能运行。sandbox 未就绪时继续创建容器没有意义，终止并等待重试是正确决策。
