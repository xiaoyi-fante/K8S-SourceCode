# 第14章 containerRuntime 创建 init 容器前期工作

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 14 章 — containerRuntime 创建 init 容器前期工作
> **源码入口**: `pkg/kubelet/kuberuntime/kuberuntime_container.go`

---

## 核心机制一览

1. **init 容器是串行有序的前置门卫**：所有 init 容器在业务容器之前按顺序逐一执行，每个必须成功退出后才启动下一个；任意一个失败都会阻止业务容器启动。

2. **SyncPod Step 6 触发 init 容器创建**：`kubeGenericRuntimeManager.SyncPod` 在步骤 6 调用 `startContainer`（与业务容器共用同一函数），通过 `findNextInitContainerToRun` 决定当前应启动哪个 init 容器。

3. **findNextInitContainerToRun 的四段式逻辑**：先检查是否有 init 容器配置 → 如果 app 容器已 Running 则 init 已全部完成 → 从后往前找最后一个 failed 的做 retry → 从后往前找第一个未完成的返回。

4. **startContainer 四步流程**：pull image → create container（`generateContainerConfig`）→ start container → run postStart lifecycle hook；本章覆盖前两步。

5. **拉取镜像三段决策**：`EnsureImageExists` 先通过 `GetImageRef` 检查本地是否存在，再由 `shouldPullImage`（PullNever/PullAlways/PullIfNotPresent）决策是否拉取，最终调用 `imageService.PullImage` gRPC；串行/并行两种 puller 都走同一路径。

6. **generateContainerConfig 的六项准备工作**：获取 `RunContainerOptions`（volumes/envs/devices）→ 获取 hostname/nodename → 校验 RunAsNonRoot → 构造 command/args → 创建日志目录（restartCount 为后缀） → 组装 ContainerConfig。

---

## 全章调用链总图

```
SyncPod (kuberuntime_manager.go:702)
  │
  ▼ Step 6: start the init container
  │  findNextInitContainerToRun (kuberuntime_container.go:771)
  │    ├─ app containers already Running? → done=true
  │    ├─ last failed init → retry (status, container, false)
  │    ├─ last exited ok → return next init container
  │    └─ fallback: return InitContainers[0]
  │
  ▼ startContainer (kuberuntime_container.go:136)
  │
  ├─ Step 1: pull image
  │    imagePuller.EnsureImageExists (image_manager.go:89)
  │      ├─ GetImageRef → imageService.ImageStatus gRPC  (kuberuntime_image.go:83)
  │      ├─ shouldPullImage (image_manager.go:65)
  │      │    PullNever → skip
  │      │    PullAlways / PullIfNotPresent+absent → pull
  │      ├─ backoff check
  │      └─ puller.pullImage → imageService.PullImage gRPC
  │           串行: serialImagePuller (one at a time)
  │           并行: parallelImagePuller (goroutine per image)
  │
  └─ Step 2: generateContainerConfig (kuberuntime_container.go:242)
       ├─ containerManager.GetResources → RunContainerOptions
       ├─ GeneratePodHostNameAndDomain → hostname / nodename
       ├─ volumeManager.GetMountedVolumesForPod → volumes
       ├─ makeEnvironmentVariables → envs
       ├─ makeMounts (kuberuntime_container.go:321)
       │    shouldMountHostsFile → /etc/hosts
       │    volume hostPath + subPath + SELinux relabel
       ├─ getImageUser → uid / username  (helpers.go:122)
       ├─ verifyRunAsNonRoot (security_context_others.go:30)
       ├─ ExpandContainerCommandAndArgs (container/helpers.go:163)
       └─ BuildContainerLogsDirectory (helpers.go:173)
            MkdirAll(logDir, 0755)
            containerLogsPath = logDir/restartCount
```

---

## §01 init 容器的作用与应用场景

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| SyncPod Step 6 init 容器入口 | [kuberuntime_manager.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_manager.go) | `SyncPod:702` |

### init 容器是什么

Init 容器是一种特殊容器，在 Pod 内的业务容器启动之前运行，专门用于做初始化工作。与普通容器的核心区别：

- init 容器**串行**执行，每个必须成功退出后才启动下一个；普通容器**并行**启动
- init 容器没有 liveness/readiness/startup probe
- init 容器失败后，kubelet 根据 Pod 的 `restartPolicy` 决定是否重启（`Never` 则 Pod 失败终止）

```
Pod 生命周期时序：

  Pause 容器（网络命名空间锚）
    │
    ▼ 容器环境初始化
    │
    Init C[0] ──完成──▶ Init C[1] ──完成──▶ ... ──完成──▶
                                                          │
                                                          ▼
                                               Main Container
                                                 (Liveness / Readiness / Stop)
```

### 典型应用场景

**等待服务依赖 Ready**：init 容器不断轮询 DNS，等到依赖服务可解析后退出，再交由主容器启动。

```yaml
initContainers:
- name: init-myservice
  image: busybox
  command: ['sh', '-c', 'until nslookup myservice; do echo waiting; sleep 2; done']
- name: init-mydb
  image: busybox
  command: ['sh', '-c', 'until nslookup mydb; do echo waiting; sleep 2; done']
```

**做初始化配置**：init 容器通过 `emptyDir` 共享卷向主容器注入配置文件，例如用 `wget` 拉取远程配置写入 `/work-dir`，主容器的 nginx 挂载同一 emptyDir 读取 `index.html`。

---

## §02 创建 init 容器步骤 1——拉取镜像

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| init 容器选择逻辑 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `findNextInitContainerToRun:771` |
| startContainer 入口 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `startContainer:136` |
| 镜像存在性检查 | [kuberuntime_image.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_image.go) | `GetImageRef:83` |
| 镜像管理器拉取入口 | [image_manager.go](kubernetes/pkg/kubelet/images/image_manager.go) | `EnsureImageExists:89` |
| 拉取策略判断 | [image_manager.go](kubernetes/pkg/kubelet/images/image_manager.go) | `shouldPullImage:65` |

### findNextInitContainerToRun：决定当前该启动哪个 init 容器

`SyncPod` Step 6 的代码先调用 `computePodContainerChanges.NextInitContainerToStart` 获取一个候选，再用 `findNextInitContainerToRun` 做最终决策。该函数实现"按顺序、失败重试"语义，逻辑分四段：

```
1. len(pod.Spec.InitContainers) == 0  → return nil, nil, true（无 init 容器，done）

2. 遍历 app containers：
   任一处于 Running → 说明所有 init 都已完成
   return nil, nil, true

3. 从后往前遍历 init containers，找最后一个 failed 的：
   isInitContainerFailed(status) → return status, container, false（触发 retry）

4. 从后往前遍历 init containers，跳过 status==nil（continue）：
   status.State == Running → return nil, nil, false（已在运行，等待）
   status.State == Exited  → 如果是最后一个 → return nil, nil, true（全部完成）
                             否则 → return nil, &InitContainers[i+1], false（下一个）

5. 兜底：return nil, &pod.Spec.InitContainers[0], false（从第一个开始）
```

### startContainer：与业务容器共用同一函数

`startContainer` 有四个步骤，init 容器和业务容器完全相同：

```
Step 1: pull the image       ← 本节
Step 2: create the container ← §03
Step 3: start the container
Step 4: run postStart lifecycle hook
```

### EnsureImageExists：拉取镜像的完整决策链

```go
// pkg/kubelet/images/image_manager.go:89
func (m *imageManager) EnsureImageExists(...) (string, string, error) {
    // 1. 生成容器 ref（用于 event 上报）
    ref, err := kubecontainer.GenerateContainerRef(pod, container)

    // 2. 检查镜像 tag，无 tag 则补 latest
    image, err := applyDefaultImageTag(container.Image)

    // 3. 构造 ImageSpec（含 Pod Annotations）
    spec := kubecontainer.ImageSpec{Image: image, Annotations: podAnnotations}

    // 4. 检查本地是否已有该镜像
    imageRef, err := m.imageService.GetImageRef(spec)
    present := imageRef != ""

    // 5. 决策是否需要拉取
    if !shouldPullImage(container, present) {
        // 不需要拉取：直接返回 imageRef
        return imageRef, "", nil
    }

    // 6. backoff 检查（防止频繁重试）
    if m.backOff.IsInBackOffSinceUpdate(backOffKey, m.backOff.Clock.Now()) {
        return "", msg, ErrImagePullBackOff
    }

    // 7. 调用 puller 实际拉取
    m.puller.pullImage(spec, pullSecrets, pullChan, podSandboxConfig)
}
```

**`shouldPullImage` 的三条规则**：

| 策略 | 行为 |
|------|------|
| `PullNever` | 永不拉取，直接返回 false |
| `PullAlways` | 总是拉取，返回 true |
| `PullIfNotPresent` + 本地不存在 | 返回 true；本地已存在返回 false |

### 串行 vs 并行 puller

kubelet 启动时根据 `--serialize-image-pulls`（默认 true）决定使用哪种 puller：

- **串行（serialImagePuller）**：`for pullRequest := range sip.pullRequests` — 一次只拉一张镜像，避免磁盘 I/O 争抢
- **并行（parallelImagePuller）**：`go func() { ... }()` — 每张镜像一个 goroutine 异步拉取

无论哪种，底层都调用 `imageService.PullImage`（gRPC），通过 CRI Shim 转发给容器引擎。

### kubeGenericRuntimeManager.PullImage 解析

`GetImageRef` 和 `PullImage` 底层都走同一个 `imageService`（gRPC client）：

```go
// pkg/kubelet/kuberuntime/kuberuntime_image.go:83
func (m *kubeGenericRuntimeManager) GetImageRef(image kubecontainer.ImageSpec) (string, error) {
    status, err := m.imageService.ImageStatus(toRuntimeAPIImageSpec(image))
    if status == nil {
        return "", nil   // 镜像不在本地
    }
    return status.Id, nil
}
```

`PullImage` 在拉取前先用 `parsers.ParseImageName` 解析出镜像的名字、标签、digest。digest 是镜像内容的 SHA256 哈希，保证"即使名字和 tag 不变，内容变了也拉新版"。

---

## §03 创建 init 容器步骤 2——create 的准备工作

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| 创建容器配置 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `generateContainerConfig:242` |
| volume 挂载处理 | [kuberuntime_container.go](kubernetes/pkg/kubelet/kuberuntime/kuberuntime_container.go) | `makeMounts:321` |
| 获取镜像用户 | [helpers.go](kubernetes/pkg/kubelet/kuberuntime/helpers.go) | `getImageUser:122` |
| 验证非 root | [security_context_others.go](kubernetes/pkg/kubelet/kuberuntime/security_context_others.go) | `verifyRunAsNonRoot:30` |
| 展开容器命令 | [container/helpers.go](kubernetes/pkg/kubelet/container/helpers.go) | `ExpandContainerCommandAndArgs:163` |
| 创建日志目录 | [helpers.go](kubernetes/pkg/kubelet/kuberuntime/helpers.go) | `BuildContainerLogsDirectory:173` |

### startContainer 步骤 2：create container 的入口

```go
// pkg/kubelet/kuberuntime/kuberuntime_container.go:242（简化）
containerConfig, cleanupAction, err := m.generateContainerConfig(
    container, pod, restartCount, podIP, imageRef, podIPs, nsTarget)
if cleanupAction != nil {
    defer cleanupAction()
}
```

`cleanupAction` 是收尾清理函数（如挂载失败时的 umount），`defer` 保证无论是否出错都执行。

### generateContainerConfig 的六项准备工作

**① 获取 RunContainerOptions（容器运行参数聚合体）**

`RunContainerOptions` 是容器启动前所有配置的聚合结构体（位于 `pkg/kubelet/container/runtime.go`）：

```go
type RunContainerOptions struct {
    Envs               []EnvVar     // 环境变量
    Mounts             []Mount      // volume 挂载信息
    Devices            []DeviceInfo // 节点设备映射
    Annotations        []Annotation // 容器 Annotation
    PodContainerDir    string       // 容器日志根目录
    ReadOnly           bool         // 只读文件系统
    Hostname           string       // 容器 Hostname
    EnableHostUserNamespace bool    // userns=host
}
```

通过 `containerManager.GetResources(pod, container)` 填充 Mounts/Devices 等资源相关字段。

**② 获取 hostname 和 nodename**

```go
hostname, hostDomainName, err := kl.GeneratePodHostNameAndDomain(pod)
```

默认 hostname = `pod.Name`；若 `pod.Spec.Hostname` 有值则使用指定值。`nodename` 的行为由 `SetHostnameAsFQDN` 控制：
- `SetHostnameAsFQDN=false`：nodename = hostname
- `SetHostnameAsFQDN=true`：nodename = `hostname.hostDomainName`（FQDN）

**③ 获取 Pod 绑定的 volumes**

```go
volumes := kl.volumeManager.GetMountedVolumesForPod(podName)
```

Pod 中多个容器共享同一组 volume，因此以 Pod 为粒度获取，而非单个容器。

**④ 获取环境变量**

```go
envs, err := kl.makeEnvironmentVariables(pod, container, podIP, podIPs)
opts.Envs = append(opts.Envs, envs...)
```

来源：容器配置中的 `env` 字段 + `envFrom`（ConfigMap/Secret 引用）。

**⑤ makeMounts：处理 volume 挂载与 /etc/hosts**

`makeMounts` 决定容器实际看到的挂载列表，核心逻辑：

```
是否挂载 /etc/hosts？
  shouldMountHostsFile(pod, podIPs, supportsSingleFileMapping)
  ├─ 容器是 infra (pause) 容器 → 不挂载
  ├─ 容器本就没有挂载 /etc/hosts → 不挂载
  └─ sandbox 创建时 pod ip 未知（unknown 状态） → 不挂载

对每个 volume mount：
  1. volumeutil.GetPath(vol.Mounter) → hostPath（宿主机真实路径）
  2. 处理 SubPath / SubPathExpr（变量展开 + 路径合法性校验）
  3. volumePath = hostPath / subPath
  4. SELinux 支持检查 → relabelVolume = true
  5. 处理挂载传播（propagation）：
     None (default) → 宿主看不到容器内新挂载
     HostToContainer  → 宿主新挂载向容器单向传播
     Bidirectional  → 双向传播（需要 privileged）
  6. 检查 terminationMessagePath（容器退出消息写入路径）
```

**⑥ getImageUser：从镜像元数据获取运行用户**

```go
// pkg/kubelet/kuberuntime/helpers.go:122
func (m *kubeGenericRuntimeManager) getImageUser(image string) (*int64, string, error) {
    imageStatus, err := m.imageService.ImageStatus(&runtimeapi.ImageSpec{Image: image})
    if imageStatus.Uid != nil {
        return &imageStatus.GetUid().Value, "", nil  // 优先用 uid
    }
    if imageStatus.Username != "" {
        return nil, imageStatus.Username, nil        // 其次用 username
    }
    return new(int64), "", nil  // 默认 root (uid=0)
}
```

获取的 uid/username 用于后续 `verifyRunAsNonRoot` 校验。

### verifyRunAsNonRoot：RunAsNonRoot 安全校验

```
获取 effectiveSecurityContext
  ├─ RunAsNonRoot == nil → 不校验，直接返回 nil
  ├─ RunAsUser == 0 → 错误（容器配置要以 root 运行，违反策略）
  ├─ uid != nil && *uid == 0 → 错误（镜像默认用户是 root）
  ├─ uid == nil && len(username) > 0 → 错误（username 非数字，无法判断）
  └─ 通过
```

两种情况会报错：
1. 容器配置了 `runAsNonRoot` 但镜像要求以 root 运行
2. 容器配置了 `runAsNonRoot` 但镜像返回的是 username（非数字），无法校验是否为 root

### 构造容器运行的 command

```go
// pkg/kubelet/container/helpers.go:163
func ExpandContainerCommandAndArgs(container *v1.Container, envs []EnvVar) ([]string, []string) {
    mapping := expansion.MappingFuncFor(envVarsToMap(envs))
    // 遍历 container.Command，展开其中的 $(VAR_NAME) 变量引用
    for _, cmd := range container.Command {
        command = append(command, expansion.Expand(cmd, mapping))
    }
    // 遍历 container.Args，同样展开变量
    for _, arg := range container.Args {
        args = append(args, expansion.Expand(arg, mapping))
    }
    return command, args
}
```

`Command` 和 `Args` 中都可以引用 `$(ENV_VAR)` 形式的变量，在这一步完成展开替换。

### 创建容器日志目录

```go
logDir := BuildContainerLogsDirectory(pod.Namespace, pod.Name, pod.UID, container.Name)
err = m.osInterface.MkdirAll(logDir, 0755)
containerLogsPath := buildContainerLogsPath(container.Name, restartCount)
restartCountUint32 := uint32(restartCount)
```

日志路径以 `restartCount` 为后缀，防止节点重启后 `containerStatus` 被清空时日志混乱——重启次数由 `containerStatus.RestartCount + 1` 计算，`log` 前缀就是实际重启次数。
