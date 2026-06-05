# 第21章 kubelet中内置的dockershim机制

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 21 章 — kubelet 中内置的 dockershim 机制
> **源码入口**: `pkg/kubelet/kubelet_dockershim.go`

## 核心机制一览

1. **容器的本质是受限进程**：容器通过 Linux Namespace（隔离进程视图）和 Cgroups（限制资源使用）实现进程间的隔离和约束。Namespace 是"看不见"，Cgroups 是"用不完"，两者结合就是容器。

2. **容器运行时分两层**：Low-Level 运行时（lxc、runc、imctfy）只管容器的启动运行；High-Level 运行时（containerd、cri-o）在此之上增加镜像管理和 gRPC/Web API，最终通过 OCI 规范（ImageSpec + RuntimeSpec）与 Low-Level 对接。

3. **CRI 是 kubelet 与运行时的标准接口**：kubelet 通过 gRPC 调用 CRI（Container Runtime Interface），CRI 定义了两个 gRPC 服务：`RuntimeService`（管理 Pod 和容器生命周期）和 `ImageService`（镜像拉取/查询/删除）。shim 作为 CRI 服务器，kubelet 作为客户端。

4. **dockershim 是内嵌在 kubelet 中的适配层**：由于 Docker 未实现 CRI，Kubernetes 在 kubelet 内部内置了 dockershim，将 CRI 调用翻译成 Docker API（HTTP REST）。这是一个临时方案（故名 shim），已在 v1.23/1.24 版本移除。

5. **dockershim 启动流程**：`PreInitRuntimeService`（kubelet.go:293）→ `runDockershim`（kubelet_dockershim.go:30）→ `NewDockerService`（docker_service.go:196）构造 `dockerService` 对象 → `dockerService.Start`（docker_service.go:411）启动内部服务 → 最后启动 gRPC server 同时注册 `RuntimeService` 和 `ImageService`。

6. **CRI 接口最终落到 Docker HTTP API**：以 `ListImages` 为例，调用链为 CRI gRPC → `dockerService.ListImages`（docker_image.go:38）→ `ds.client.ImageList` → `cli.get(ctx, "/images/json", ...)` HTTP 请求 Docker Daemon。`CreateContainer` 同理，最终调 `/containers/create`。

---

## 全章调用链总图

```
kubelet PreInitRuntimeService — kubelet.go:293
  │
  ▼ containerRuntime == "docker"
  │
  ▼ runDockershim() — kubelet_dockershim.go:30
  │   ├─ 构造 streamingConfig (getStreamingConfig)
  │   ├─ 构造 dockerClientConfig (&dockershim.ClientConfig)
  │   ├─ NewDockerService(...) — docker_service.go:196
  │   │     ├─ NewDockerClientFromConfig → libdocker.Interface (docker client)
  │   │     ├─ NewInstrumentedInterface (包装 client，加 metrics)
  │   │     ├─ checkpointManager.New
  │   │     ├─ 构造 &dockerService{client, streamingRuntime, containerManager, ...}
  │   │     ├─ checkVersionCompatibility (最低 Docker API 1.26)
  │   │     └─ 初始化 streamingServer
  │   │
  │   └─ dockerremote.NewDockerServer(remoteRuntimeEndpoint, ds)
  │         └─ dockerServer.Start() — docker_service.go:411
  │               ├─ ds.service.Start()
  │               │     ├─ go ds.streamingServer.Start(true)
  │               │     └─ ds.containerManager.Start()
  │               └─ 启动 gRPC server
  │                     ├─ util.CreateListener(s.endpoint)
  │                     ├─ grpc.NewServer(MaxRecvMsgSize, MaxSendMsgSize)
  │                     ├─ runtimeapi.RegisterRuntimeServiceServer(server, service)
  │                     ├─ runtimeapi.RegisterImageServiceServer(server, service)
  │                     └─ go server.Serve(l)
  │
  ▼ kubelet 通过 gRPC 调用 CRI 接口
  │
  ├─ ListImages → dockerService.ListImages — docker_image.go:38
  │     └─ ds.client.ImageList → cli.get("/images/json") [Docker HTTP API]
  │
  └─ CreateContainer → dockerService.CreateContainer — docker_container.go:109
        └─ ds.client.CreateContainer → d.client.ContainerCreate
              └─ cli.post("/containers/create") [Docker HTTP API]
```

---

## §01. 容器和 Namespace

**容器的本质是受限制的进程**：利用 Linux 内核的 Namespace 和 Cgroups，让进程之间彼此隔离，就称为容器。

**Namespace 是一种资源隔离方案**：在同一个 Namespace 下的进程可以感知彼此的存在和变化，对 Namespace 外的进程一无所知。Linux 提供 6 种 Namespace：

| Namespace | 隔离内容 |
|-----------|---------|
| PID | 进程 ID，每个 Namespace 拥有独立计数器 |
| Mount | 文件系统挂载点，容器内 `/proc/mounts` 只看到自己的挂载 |
| IPC | 进程间通信（信号量、消息队列、共享内存） |
| UTS | 主机名和域名信息（`hostname` 命令） |
| Network | 网络资源（IP、网卡、路由、端口） |
| User | 用户 ID 和用户组 ID（容器内 UID 1000 对应宿主机不同 UID） |

**PID Namespace 示例**：在 UNIX 系统中，PID 1 是所有进程的祖先。在独立 PID Namespace 中，每个 Namespace 自己从 1 开始计数——Docker 的第一个进程就是 dockerinit，之后再启动 bash，所以 bash PID 为 2。

**Mount Namespace 示例**：通过 `/proc/<pid>/mounts` 可以查看指定进程所属 Namespace 下的文件系统挂载情况，比如 apiserver 进程有独立的 overlay 文件系统挂载。

**User Namespace**：容器内的 UID 与宿主机的 UID 可以不同。主要目的是提升安全性，让容器内 root 用户（UID 0）映射到宿主机上的普通用户。容器没有自己的 User Namespace 时，容器内的 UID 就是宿主机上的 UID。

**容器 vs 虚拟机的差距**：Namespace 的隔离是不彻底的。比如时间（`/proc/timer_list`）不能被隔离，一旦某个容器修改了时间，会影响宿主机上的全部容器；又如在容器内 `top` 看到的是宿主机全局的进程信息，而非只有本 Namespace 中的进程。

---

## §02. 容器和 Cgroups

**Linux Cgroups（Control Group）**：Linux 内核的重要功能，通过为进程设置资源限制来隔离宿主机上的物理资源，例如 CPU、内存、磁盘 I/O 和网络带宽。

**Cgroup 子系统**（每个子系统是一个资源控制器）：

| 子系统 | 功能 |
|--------|------|
| cpu | 限制进程的 CPU 使用率 |
| cpuacct | 统计 cgroups 中进程的 CPU 使用报告 |
| cpuset | 为 cgroups 中的进程分配独立 CPU 节点或内存节点 |
| memory | 限制进程的内存使用量 |
| blkio | 限制进程的块设备 IO |
| devices | 控制进程能访问哪些设备 |
| net_cls | 标记 cgroups 中进程的网络数据包，供 tc（traffic control）进行流量控制 |
| net_prio | 限制进程网络流量的优先级 |
| huge_tlb | 限制 HugeTLB 的使用 |
| freezer | 挂起或恢复 cgroups 中的进程 |
| ns | 控制 cgroups 中的进程使用不同的 Namespace |

通过 `mount -t cgroup` 可以看到系统上每个子系统对应的目录，目录下的文件就是该子系统的控制文件，向其写入值即可限制对应资源的使用。

---

## §03. 容器运行时的乱战

### 容器运行时的定义

容器是利用 Linux Namespace 和 Cgroups 实现的受限进程。**容器运行时（container runtime）负责一个容器运行的所有部分**，凡是能够完成这个功能的应用都可以称为容器运行时。

### Low-Level 与 High-Level 运行时

```
Low Level ←————————————————————————————→ High Level
    ├── lxc
    ├── runc
    ├── imctfy
                    ├── cri-o
                    ├── containerd
                    └── rkt
```

- **Low-Level 运行时**：专注于容器的运行本身，直接操作 Namespace/Cgroups。
- **High-Level 运行时**：支持更多高级功能（镜像管理和 gRPC / Web API），将容器运行的实现外包给 Low-Level 运行时（通过 OCI 接口）。

**两层关系**：High-Level 调用 Low-Level，不可绕过，也不互斥。CLI 等上层工具通过 API 调用 High-Level Runtime，High-Level Runtime 从 Registry 拉取镜像后通过 OCI 接口调用 Low-Level Runtime，Low-Level Runtime 最终创建容器。

### OCI（Open Container Initiative）规范

OCI 规定了两点：
- **ImageSpec**：容器镜像长什么样子
- **RuntimeSpec**：容器要接收哪些指令，以及这些指令对应的行为

OCI 诞生的背景：早期 Kubernetes 硬编码调用 Docker API，随着 Docker 的发展和 Google 的主导，业界建立了 OCI 规范，以防止 Docker 一家公司控制整个生态。

### 主要运行时

**imctfy**：Google 内部项目，brog 系统内部使用的容器运行时正是 imctfy。它最有个性的特性是将每个子系统的 cgroups 挂载在同一个命名空间下，防止子 cgroups 之间相互争抢资源。

**runc**：OCI 标准的官方实现，所有的容器都可以随便迁移。Docker 最初使用 libcontainer 管理容器，后来将 libcontainer 开源并捐出，最终成为 runc。

**rkt**：CoreOS 主导开发，用于替代 Docker/runc 的流行方案，既提供低级又提供高级运行时。

**Docker**：最早的开源容器运行时之一，整合了构建和运行容器生命周期中所需的全部功能。从 Docker 1.11 版本开始，Docker 运行时不再是简单地通过 Docker Daemon 来启动，而是通过集成 containerd、runc 来运行。

**Docker 创建容器流程**（6步）：

```
① kubelet 调 CRI (gRPC) → dockershim
② dockershim 调 Docker Daemon
③ Docker Daemon 通知 containerd 启动一个容器
④ containerd 创建 containerd-shim 子进程管理容器
⑤ containerd-shim 使用 runc 启动容器
⑥ runc 启动完成后退出，containerd-shim 成为容器进程父进程，
   负责收集容器进程状态上报 containerd，并在 pid 1 进程退出后
   接管容器中的子进程清理
```

**Docker 的悲哀**：将容器操作迁移到 containerd 是因为当时做 Swarm，想进军 PaaS 市场。让 Docker Daemon 专门负责上层封装编排，但后来 Swarm 在 Kubernetes 面前惨败，Docker 公司把 containerd 项目捐赠给了 CNCF 基金会。

**containerd**：从 Docker 分离出来的高级运行时，与 runc 一样被分解为独立组件。containerd 只专注于运行中的容器管理，不提供用户和开发人员工具（`docker ps`、`docker inspect` 等由 Docker 提供）。

---

## §04. k8s 的 CRI 接口和 dockershim 的去留

### CRI 诞生背景

Kubernetes 早期硬编码支持 Docker，通过硬编码直接调用 Docker API。随着 Docker 的不断发展以及 Google 的主导，出现了更多容器运行时，Kubernetes 为了支持更多更简洁的容器运行时，提出了 CRI 标准。

### CRI 接口定义

CRI（Container Runtime Interface）是 Kubernetes 定义的一组与容器运行时进行通信的接口，kubelet 通过 gRPC 框架与容器运行时或 shim 进行通信（kubelet 为客户端，CRI shim 为服务器）。

两个 gRPC 服务：

```
                        gRPC Server
┌────────────────────────────────────────────────────────┐
│  RuntimeService                  ImageService          │
│  ┌──────────────┬──────────────┐  ┌──────────────────┐ │
│  │RunPodSandbox │CreateContainer│  │ListImages        │ │
│  │StopPodSandbox│StartContainer │  │PullImage         │ │
│  │RemovePodSand.│StopContainer  │  │ImageStatus       │ │
│  │PodSandboxStat│RemoveContainer│  │RemoveImage       │ │
│  │ListPodSandbox│ListContainers │  │ImageFsInfo       │ │
│  │UpdateRuntime │ContainerStatus│  └──────────────────┘ │
│  │ContainerStats│Version/Status │                       │
│  │              │Exec/ExecSync  │                       │
│  │              │Attach/PortFwd │                       │
│  └──────────────┴──────────────┘                       │
└────────────────────────────────────────────────────────┘
```

- `ImageService`：负责镜像的拉取、查询和删除
- `RuntimeService`：负责 Pod 和容器的生命周期管理，以及与容器的交互（exec/attach/port-forward）

### k8s 为何内置 dockershim

Docker 没有实现 CRI，k8s 为了麻痹 Docker 在代码中直接内置了 dockershim，将 CRI 调用翻译为 Docker API 调用。使用 Docker 创建容器需要 6 步，比较繁琐，切换到 containerd 可以消掉中间环节（dockershim 和 Docker Daemon）。

### 切换到 containerd

```
Docker 路径：
kubelet ─CRI→ dockershim ─API→ Docker Daemon ─→ containerd ─→ container

containerd 1.0 路径（containerd 实现了一个单独的 CRI-Containerd 进程）：
kubelet ─CRI→ CRI-Containerd ─→ containerd ─→ container

containerd 1.1 路径（CRI 插件直接内置在 containerd 中）：
kubelet ─CRI→ containerd（内置 CRI 插件）─→ container
```

切换到 containerd 后不能再使用的 docker 工具：`docker ps`、`docker inspect`、`docker exec`（需改用 crictl 等 CRI 兼容工具）。

### k8s 废弃 dockershim 的原因

- 维护 dockershim 已成为 Kubernetes 维护者肩头一沉重的负担
- 创建 CRI 标准就是为了减轻这个负担，同时增加不同容器运行时之间的平滑互操作性
- Docker 迟迟没有实现 CRI
- dockershim 与不兼容的一些特性（cgroups v2、User Namespace）已在新 CRI 运行时实现，移除 dockershim 可加速这些领域的发展
- 计划在 v1.23/1.24 版本下线

**切换注意事项**：现有的 Docker 镜像能正常工作；需注意运行时日志格式变化、无法用 docker 相关命令直接控制容器、需要确认 Kubernetes 工具（`kube-imageputpuller`）兼容性、registry mirrors 和安全配置需重新配置。

---

## §05. kubelet 中 dockershim 源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| dockershim 启动入口 | [kubelet_dockershim.go](kubernetes/pkg/kubelet/kubelet_dockershim.go) | `runDockershim:30` |
| 运行时类型判断 | [kubelet.go](kubernetes/pkg/kubelet/kubelet.go) | `PreInitRuntimeService:293` |
| dockerService 构造 | [docker_service.go](kubernetes/pkg/kubelet/dockershim/docker_service.go) | `NewDockerService:196` |
| dockerService 启动 | [docker_service.go](kubernetes/pkg/kubelet/dockershim/docker_service.go) | `Start:411` |
| Docker 版本兼容检查 | [docker_service.go](kubernetes/pkg/kubelet/dockershim/docker_service.go) | `checkVersionCompatibility:484` |
| ListImages 实现 | [docker_image.go](kubernetes/pkg/kubelet/dockershim/docker_image.go) | `ListImages:38` |
| CreateContainer 实现 | [docker_container.go](kubernetes/pkg/kubelet/dockershim/docker_container.go) | `CreateContainer:109` |

### 入口：PreInitRuntimeService → runDockershim

kubelet 的 `PreInitRuntimeService`（kubelet.go:293）根据命令行参数 `--container-runtime` 决定运行时类型：

```go
// pkg/kubelet/kubelet.go:293
func PreInitRuntimeService(...) error {
    switch containerRuntime {
    case kubetypes.DockerContainerRuntime:
        klog.InfoS("Using dockershim is deprecated, ...")
        if err := runDockershim(kubeCfg, kubeDeps, crOptions, ...); err != nil {
            return err
        }
    case kubetypes.RemoteContainerRuntime:
        // No-op. 其他 runtime 已实现 CRI gRPC 接口，kubelet 直接连接
    default:
        return fmt.Errorf("unsupported CRI runtime: %q", containerRuntime)
    }
}
```

`--container-runtime` 只有两个合法值：`docker`（启动内置 dockershim grpc）和 `remote`（其他 runtime 已实现 CRI 的 grpc 接口，直接连接）。

### runDockershim：为启动 gRPC server 做准备

```go
// pkg/kubelet/kubelet_dockershim.go:30
func runDockershim(kubeCfg ...) error {
    // 1. 初始化 network 插件设置
    pluginSettings := dockershim.NetworkPluginSettings{
        HairpinMode:        kubeletconfiginternal.HairpinMode(kubeCfg.HairpinMode),
        PluginName:         crOptions.NetworkPluginName,
        PluginConfDir:      crOptions.CNIConfDir,
        // ...
    }

    // 2. 初始化 streaming 配置（exec/attach/port-forward 的 streaming server）
    streamingConfig := getStreamingConfig(kubeCfg, kubeDeps, crOptions)

    // 3. 初始化 docker client 配置
    dockerClientConfig := &dockershim.ClientConfig{
        DockerEndpoint:      kubeDeps.DockerOptions.DockerEndpoint,
        RuntimeRequestTimeout: kubeDeps.DockerOptions.RuntimeRequestTimeout,
        // ...
    }

    // 4. 构造集成 CRI RuntimeService 和 ImageService 的 dockerService 对象
    ds, err := dockershim.NewDockerService(dockerClientConfig, crOptions.PodSandboxImage,
        streamingConfig, &pluginSettings, runtimeCgroups, ...)
}
```

### NewDockerService：构造 dockerService

`NewDockerService`（docker_service.go:196）构造 `dockerService` 对象，该对象同时实现了 `RuntimeService` 和 `ImageService` 两个接口：

```go
// pkg/kubelet/dockershim/docker_service.go:196
func NewDockerService(config *ClientConfig, ...) (DockerService, error) {
    client := NewDockerClientFromConfig(config)
    c := libdocker.NewInstrumentedInterface(client)  // 加 metrics 包装
    checkpointManager, _ := checkpointmanager.NewCheckpointManager(...)
    ds := &dockerService{
        client:            c,
        os:                kubecontainer.RealOS{},
        podSandboxImage:   podSandboxImage,
        streamingRuntime:  &streamingRuntime{client: client, execHandler: &NativeExecHandler{}},
        containerManager:  cm.NewContainerManager(cgroupsName, client),
        checkpointManager: checkpointManager,
        networkReady:      make(map[string]bool),
        containerCleanupInfos: make(map[string]*containerCleanupInfo),
    }
    // 检查 Docker 版本兼容性（最低 API 版本 1.26）
    if err := ds.checkVersionCompatibility(); err != nil {
        return nil, err
    }
    // 初始化 streaming server（用于 exec/attach/port-forward）
    if streamingConfig != nil {
        ds.streamingServer, _ = streaming.NewServer(*streamingConfig, ds.streamingRuntime)
    }
    // 初始化 CNI 插件（hairpin mode 等网络设置）
    // 初始化 cgroupDriver
    return ds, nil
}
```

**checkVersionCompatibility**（docker_service.go:484）：通过 Docker API 查询当前版本，与 `libdocker.MinimumDockerAPIVersion` 对比，版本过低直接返回错误。

### dockerService.Start：启动内部服务和 gRPC server

```go
// pkg/kubelet/dockershim/docker_service.go:411
func (ds *dockerService) Start() error {
    ds.initCleanup()

    go func() {
        if err := ds.streamingServer.Start(true); err != nil {
            // streaming server 停止则 kubelet 退出
            os.Exit(1)
        }
    }()

    return ds.containerManager.Start()  // 启动 container manager（cgroup 管理）
}
```

然后启动 dockershim 的 gRPC server：

```go
// pkg/kubelet/dockershim/remote/docker_server.go（简化）
klog.V(2).InfoS("Start dockershim grpc server")
l, err := util.CreateListener(s.endpoint)  // 监听 Unix socket
s.server = grpc.NewServer(
    grpc.MaxRecvMsgSize(maxMsgSize),
    grpc.MaxSendMsgSize(maxMsgSize),
)
// 同时注册 RuntimeService 和 ImageService，ds 实现了两个接口
runtimeapi.RegisterRuntimeServiceServer(s.server, s.service)
runtimeapi.RegisterImageServiceServer(s.server, s.service)
go func() {
    if err := s.server.Serve(l); err != nil {
        klog.ErrorS(err, "Failed to serve connections")
        os.Exit(1)
    }
}()
```

### CRI 接口实现举例

**ListImages**（docker_image.go:38）：

```go
func (ds *dockerService) ListImages(_ context.Context, r *runtimeapi.ListImagesRequest) (*runtimeapi.ListImagesResponse, error) {
    filter := dockertypes.ImageListOptions{}
    if r.GetFilter() != nil {
        filter.Filters = dockerfilters.NewArgs()
        filter.Filters.Add("reference", r.GetFilter().GetImage().GetImage())
    }
    images, err := ds.client.ImageList(filter)
    // 将 docker API Image 格式转换为 runtimeapi.Image 格式
    result := make([]*runtimeapi.Image, 0, len(images))
    for _, img := range images {
        apiImage, err := imageToRuntimeAPIImage(&img)
        result = append(result, apiImage)
    }
    return &runtimeapi.ListImagesResponse{Images: result}, nil
}
```

底层通过 `cli.get(ctx, "/images/json", query, nil)` 调用 Docker 的 HTTP API。

**CreateContainer**（docker_container.go:109）：

```go
func (ds *dockerService) CreateContainer(_ context.Context, r *runtimeapi.CreateContainerRequest) (*runtimeapi.CreateContainerResponse, error) {
    createResp, createErr := ds.client.CreateContainer(createConfig)
    // 处理冲突（镜像 digest 命名与容器名冲突时重试）
    if createErr != nil {
        createResp, createErr = recoverFromCreationConflictIfNeeded(ds.client, createConf)
    }
    // ...
}
```

`ds.client.CreateContainer` → `d.client.ContainerCreate`（kubeDockerClient）→ `cli.post(ctx, "/containers/create", ...)` 调用 Docker HTTP API 创建容器。容器创建时自动设置默认的 shm size（若 HostConfig.ShmSize <= 0 则使用 defaultShmSize）。

整个调用链印证了 dockershim 的本质：**它是一个 gRPC CRI 服务器，内部把每个 CRI 调用翻译成 Docker HTTP REST API 调用。**
