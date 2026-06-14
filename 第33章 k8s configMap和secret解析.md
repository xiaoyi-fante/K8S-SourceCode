# 第33章 k8s ConfigMap 和 Secret 解析

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 33 章 — k8s configMap 和 secret 解析
> **源码入口**: `pkg/volume/configmap/configmap.go → SetUpAt:187` / `pkg/kubelet/volumemanager/reconciler/reconciler.go → Run:145`

---

## 核心机制一览

1. **ConfigMap 和 Secret 都是 key-value 存储，挂载为文件的本质是写磁盘**：kubelet 通过 volumeManager 的 reconciler 循环，将 ConfigMap/Secret 的内容以文件形式写入宿主机 `/var/lib/kubelet/pods/{pod_uid}/kubernetes.io~configmap/{volume_name}/` 目录，容器通过 volumeMount 看到这些文件。

2. **热更新由 reconciler 轮询驱动**：reconciler 每隔 `loopSleepDuration`（默认 100ms）执行一次 `reconcile()`，检查 desiredStateOfWorld 与 actualStateOfWorld 的差异，若 ConfigMap/Secret 内容有变化则重新写入新文件（原子写：先写隐藏目录，再用 symlink 切换）。热更新延迟 = reconciler 轮询周期（100ms 量级）。

3. **configMap volume 的 symlink 原子切换**：实际内容写到带时间戳的隐藏目录（如 `..2021_11_01_06_39_32.947090751/`），然后 `..data` symlink 指向该目录，`redis.conf` 再 symlink 指向 `..data/redis.conf`。更新时先写新目录，再原子切换 `..data` 指针，保证读到的配置文件始终是完整的一个版本。

4. **Secret 与 ConfigMap 的关键区别**：Secret 支持 Base64 编码（数据存储时 Base64，挂载后自动解码为原始内容）；Secret 支持与 ServiceAccount 关联，Pod 使用 serviceAccount 时对应 secret 自动挂载到 `/run/secrets/kubernetes.io/serviceaccount/`；Secret 有 `kubernetes.io/dockerconfigjson` 等分类型，ConfigMap 不区分类型。

5. **volumeManager 三件套**：`desiredStateOfWorld`（期望挂载哪些 volume）、`actualStateOfWorld`（实际已挂载哪些 volume）、`reconciler`（调谐两者差异：挂载缺少的 volume、卸载多余的 volume）。

---

## 全章调用链总图

```
kubelet 启动
  │
  ▼ volumeManager.Run(sourcesReady, stopCh)
  │  go vm.desiredStateOfWorldPopulator.Run(sourcesReady, stopCh)
  │  go vm.reconciler.Run(stopCh)
  │
  ▼ reconciler.Run（reconciler.go:145）
  │  wait.Until(rc.reconciliationLoopFunc(), rc.loopSleepDuration, stopCh)
  │
  ▼ reconciliationLoopFunc（:149）→ rc.reconcile() 每 100ms 执行一次
  │
  ▼ reconcile（:163）
  │  rc.unmountVolumes()        卸载多余 volume
  │  rc.mountAttachVolumes()    挂载缺少 volume
  │  rc.unmountDetachDevices()  卸载块设备
  │
  ▼ mountAttachVolumes（:202）
  │  遍历 desiredStateOfWorld 中所有 volume
  │  若 !volMounted → rc.operationExecutor.MountVolume(...)
  │
  ▼ operationExecutor.MountVolume
  │  fsVolume → operationGenerator.GenerateMountVolumeFunc
  │    volumePlugin.NewMounter → configMapVolumeMounter / secretVolumeMounter
  │    volumeMounter.SetUpAt(dir, mounterArgs)
  │
  ▼ configMapVolumeMounter.SetUpAt（configmap.go:187）
  │  b.getConfigMap(namespace, name)          从 apiserver 或 cache 获取 ConfigMap
  │  MakePayload(items, configMap, mode, opt)  提取 key→内容 map
  │  volumeutil.MakeNestedMountpoints(...)     mkdir 挂载目录
  │  writer.Write(configmap_payload)           原子写文件（symlink 切换）

configMap 热更新路径（完全相同的流程）：
  reconciler.reconcile() → mountAttachVolumes
    → configMapVolumeMounter.SetUpAt
      → 获取最新 ConfigMap → 重新 Write（覆盖 symlink）
```

---

## §01 k8s ConfigMap 简介

### ConfigMap 是什么

ConfigMap 提供向容器注入配置信息的能力，不仅可以保存单个键值属性，也可以保存整个配置文件（如 nginx.conf、redis.conf）。它将配置与镜像解耦，同一镜像可使用不同 ConfigMap 部署到不同环境。

### 创建 ConfigMap 的三种方式

**方式一：从目录创建**

```bash
# 准备配置文件
mkdir myconfigmap
cat <<EOF> myconfigmap/redis.conf
host=127.0.0.1
port=6379
EOF
cat <<EOF> myconfigmap/mysql.conf
host=127.0.0.1
port=6379
EOF

# 从目录创建 ConfigMap：目录下每个文件 → 一个 key（文件名），value 为文件内容
kubectl create configmap cm-demo01 --from-file=myconfigmap

# 查看结果
kubectl describe cm cm-demo01
# Data
# ====
# mysql.conf: host=127.0.0.1 / port=6379
# redis.conf: host=127.0.0.1 / port=6379
```

**方式二：使用 YAML 文件创建**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nginx-configmap01
data:
  nginx.conf: |
    worker_processes  1;
    events {
      worker_connections  1024;
    }
    http {
      default_type  application/octet-stream;
      sendfile      on;
      keepalive_timeout  65;
      server {
        listen       80;
        server_name  localhost;
        location / {
          root   html;
          index  index.html index.htm;
        }
        error_page   500 502 503 504  /50x.html;
        location = /50x.html {
          root  html;
        }
      }
    }
```

**方式三：直接使用字符串创建**

```bash
# --from-literal 可使用多次
kubectl create configmap cm-demo03 \
  --from-literal=db.host=localhost \
  --from-literal=db.port=3306

kubectl describe configmap/cm-demo03
# Data: db.host=localhost / db.port=3306
```

### Pod 使用 ConfigMap 的三种方式

**方式一：设置环境变量的值**

```yaml
# 单个 key 注入（configMapKeyRef）
env:
  - name: DB_HOST
    valueFrom:
      configMapKeyRef:
        name: cm-demo03
        key: db.host
  - name: DB_PORT
    valueFrom:
      configMapKeyRef:
        name: cm-demo03
        key: db.port
# 整个 ConfigMap 注入（envFrom）
envFrom:
  - configMapRef:
      name: cm-demo01
```

**方式二：在容器里设置命令行参数**

```yaml
command: ["/bin/sh", "-c", "echo $(DB_HOST) $(DB_PORT)"]
env:
  - name: DB_HOST
    valueFrom:
      configMapKeyRef:
        name: cm-demo03
        key: db.host
```

**方式三：在数据卷里面创建 config 文件（最常用）**

```yaml
# 将 ConfigMap 挂载为文件，键=文件名，值=文件内容
spec:
  containers:
    - name: testcm03
      image: busybox
      command: ["/bin/sh", "-c", "cat /etc/config/redis.conf"]
      volumeMounts:
        - name: config-volume
          mountPath: /etc/config
  volumes:
    - name: config-volume
      configMap:
        name: cm-demo02     # 该 cm 通过 --from-file=redis.conf 创建
```

查看日志，可以看到 `/etc/config/redis.conf` 内容被输出：
```
host=127.0.0.1
port=6379
```

---

## §02 k8s Secret 简介

### Secret 与 ConfigMap 的相同点

| 特性 | ConfigMap | Secret |
|------|-----------|--------|
| 存储格式 | key/value | key/value（value Base64 编码） |
| 作用域 | 某个 namespace | 某个 namespace |
| 导出为环境变量 | 支持 | 支持 |
| 通过目录/文件挂载 | 支持 | 支持 |
| volume 挂载热更新 | 支持 | 支持 |

### Secret 与 ConfigMap 的不同点

- **Secret 可被 ServiceAccount 关联**：Pod 使用 serviceAccount 时，对应的 secret 会自动挂载到 `/run/secrets/kubernetes.io/serviceaccount/` 目录
- **Secret 可存储 docker register 鉴权信息**，用于 `imagePullSecrets` 参数拉取私有镜像
- **Secret 支持 Base64 加密**
- **Secret 有分类型**：`kubernetes.io/service-account-token`、`kubernetes.io/dockerconfigjson`、`Opaque` 等；ConfigMap 不区分类型

### k8s 内置 Secret 类型

| 内置类型 | 用法 |
|---------|------|
| Opaque | 用户定义的任意数据 |
| kubernetes.io/service-account-token | 服务账号令牌 |
| kubernetes.io/dockercfg | `~/.dockercfg` 文件的序列化形式 |
| kubernetes.io/dockerconfigjson | `~/.docker/config.json` 文件的序列化形式 |
| kubernetes.io/basic-auth | 用于基本身份认证的凭据 |
| kubernetes.io/ssh-auth | 用于 SSH 身份认证的凭据 |
| kubernetes.io/tls | 用于 TLS 客户端或服务器端的数据 |
| bootstrap.kubernetes.io/token | 启动引导令牌数据 |

### 01 Opaque Secret

Opaque 类型的数据是一个 map 类型，value 必须是 Base64 编码格式：

```bash
# 对用户名/密码做 Base64 编码
$ echo -n "admin"   | base64    # YWRtaW4=
$ echo -n "admin321"| base64    # YWRtaW4zMjE=
```

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: mysecret01
type: Opaque
data:
  username: YWRtaW4=
  password: YWRtaW4zMjE=
```

`kubectl describe secret mysecret01` 只显示字节数，不显示内容；需要 `-o yaml` 才能看到 Base64 编码的值。

**以环境变量的形式使用**（使用 `secretKeyRef`，与 ConfigMap 的 `configMapKeyRef` 类比）：

```yaml
env:
  - name: USERNAME
    valueFrom:
      secretKeyRef:
        name: mysecret01
        key: username
  - name: PASSWORD
    valueFrom:
      secretKeyRef:
        name: mysecret01
        key: password
```

Pod 日志可看到 `USERNAME=admin` 和 `PASSWORD=admin321`（kubelet 挂载时自动 Base64 解码）。

**以 Volume 形式挂载**：

```yaml
volumes:
  - name: secrets
    secret:
      secretName: mysecret01
volumeMounts:
  - name: secrets
    mountPath: /etc/secrets
```

Pod 启动后 `/etc/secrets/` 下会有 `password` 和 `username` 两个文件，内容为解码后的原始字符串。

### 02 kubernetes.io/dockerconfigjson

主要用于配置拉取私有镜像的认证：

```bash
kubectl create secret docker-registry myregistry \
  --docker-server=1.1.1.1 \
  --docker-username=root \
  --docker-password=admin321 \
  --docker-email=xxxx@qq.com
```

`-o yaml` 输出后，`data.dockerconfigjson` 字段是一个 Base64 编码的 JSON，解码后结构为：
```json
{"auths":{"1.1.1.1":{"username":"root","password":"admin321","email":"...","auth":"..."}}}
```

在 Pod 中通过 `imagePullSecrets` 引用：
```yaml
spec:
  imagePullSecrets:
    - name: myregistrykey
  containers:
    - name: foo
      image: 192.168.1.100:5000/test:v1
```

### 03 kubernetes.io/service-account-token

用于被 ServiceAccount 引用。ServiceAccount 创建时 Kubernetes 会自动创建对应的 secret，Pod 如果使用了 serviceAccount，对应的 secret 会自动挂载到 `/run/secrets/kubernetes.io/serviceaccount/` 目录。

创建 ServiceAccount + ClusterRole + ClusterRoleBinding：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: mysa01
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: myclusterRoleBinding01
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: myclusterrole01
subjects:
  - kind: ServiceAccount
    name: mysa01
    namespace: default
```

Pod 使用该 SA 后，exec 进入容器可看到：
```bash
ls /run/secrets/kubernetes.io/serviceaccount
# ca.crt -> ../data/ca.crt
# namespace -> ../data/namespace
# token -> ../data/token

cat /run/secrets/kubernetes.io/serviceaccount/token
# eyJhbGci...（JWT token，可用于请求 apiserver）
```

---

## §03 kubelet volumeManager 挂载 configMap/secret 源码解读

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| reconciler 启动 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `Run:145` |
| 调谐循环 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `reconciliationLoopFunc:149` |
| 主调谐逻辑 | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `reconcile:163` |
| 挂载 volume | [reconciler.go](kubernetes/pkg/kubelet/volumemanager/reconciler/reconciler.go) | `mountAttachVolumes:202` |
| configMap SetUp | [configmap.go](kubernetes/pkg/volume/configmap/configmap.go) | `SetUpAt:187` |
| secret SetUp | [secret.go](kubernetes/pkg/volume/secret/secret.go) | `SetUpAt:182` |

### volumeManager 三件套

在第6章讲过 volumeManager 的作用：

- **kubelet** 使用 volumeManager，7 个 pods 备存储设备，存储设备将被存储在日节点存储设备在 pod 的节点上
- **volumeManager 要解决的问题**：
  - `desiredStateOfWorld`（期望状态）：实际上就是 volume，计划对 `n` 个 node 做 pod mount
  - `reconciler`（主要协程）：负责让 `actualStateOfWorld`（实际状态）向 `desiredStateOfWorld`（期望状态）靠齐
- `desiredStateOfWorldPopulator` 持续从 pod 列表填充 desiredStateOfWorld

### reconciler 的作用

- 进行实际的 volume 操作：`desiredStateOfWorld` 没有发生实质变化的 volume 就不处理
- reconciler 工作主要是让 `actualStateOfWorld` 与 `desiredStateOfWorld` 保持一致，最后还会调用 flatMap，并挂载对应路径，执行 mount

### reconciler 的启动

`volumeManager.Run` 中入口地址：`pkg/kubelet/volumemanager/volume_manager.go`

```go
go vm.reconciler.Run(stopCh)
```

具体的 Run（`reconciler.go:145`）：

```go
func (rc *reconciler) Run(stopCh <-chan struct{}) {
    wait.Until(rc.reconciliationLoopFunc(), rc.loopSleepDuration, stopCh)
}

func (rc *reconciler) reconciliationLoopFunc() func() {
    return func() {
        rc.reconcile()

        // Sync the state with the reality once after all existing pods are added to the desired state
        if rc.populatorHasAddedPods() && !rc.StatesHasBeenSynced() {
            klog.InfoS("Reconciler: start to sync state")
            rc.sync()
        }
    }
}
```

`loopSleepDuration` 默认 100ms，即每 100ms 执行一次 `reconcile()`。

### reconcile 方法分析

```
reconcile()
  │
  ├─── unmountVolumes()      卸载多余 volume（actualStateOfWorld 有但 desiredStateOfWorld 没有）
  ├─── mountAttachVolumes()  挂载缺少 volume（desiredStateOfWorld 有但 actualStateOfWorld 没有）
  └─── unmountDetachDevices()
```

挂载相关的核心在 `mountAttachVolumes`（`reconciler.go:202`），其中在 `!volMounted` 分支执行：

```go
err = rc.operationExecutor.MountVolume(
    rc.waitForAttachTimeout,
    volumeToMount.VolumeToMount,
    rc.actualStateOfWorld,
    isRemount,
)
```

### GenerateMountVolumeFunc：找到对应 volume 插件

追踪 `rc.operationExecutor.MountVolume` 发现在文件系统类型中执行了 mount 操作，调用 `operationGenerator.GenerateMountVolumeFunc`：

```go
func (oe *operationExecutor) MountVolume() {
    if fsVolume {
        // Filesystem volume case
        generatedOperations = oe.operationGenerator.GenerateMountVolumeFunc(
            waitForAttachTimeout, volumeToMount, actualStateOfWorld, isRemount)
    }
}
```

`GenerateMountVolumeFunc` 的核心：

```go
// 找到 volumePlugin（根据 volumeType 找对应插件）
volumePlugin, err := og.volumePluginMgr.FindPluginBySpec(volumeToMount.VolumeSpec)

// 获取一个 mounter 对象
volumeMounter, newMounterErr := volumePlugin.NewMounter(
    volumeToMount.VolumeSpec,
    volumeToMount.Pod,
    volume.VolumeOptions{},
)

// 设置
volumeMounter.SetUp(volume.MounterArgs{...})
```

ConfigMap 对应的 `volumePlugin` 就是 `configMapPlugin`，它返回的 mounter 是 `configMapVolumeMounter`。

### 追踪 configMapVolumeMounter 的 SetUp

首先创建一个 configMap 和一个挂载它的 pod：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: myconfigmap01
data:
  redis.conf: |
    host=127.0.0.1
    port=6379
---
apiVersion: v1
kind: Pod
metadata:
  name: myconfigmap01-pod
spec:
  containers:
    - name: web
      image: nginx:1.8
      ports:
        - containerPort: 80
          name: "http-server"
      volumeMounts:
        - name: config-volume
          mountPath: /etc/config
  volumes:
    - name: config-volume
      configMap:
        name: myconfigmap01
```

创建后获取 pod uid，到对应节点查找：

```bash
kubectl get pod/myconfigmap01-pod -o yaml | grep uid
# uid: d475ecaf-ce2e-4eac-af41-2d13bda6e0ec

# 在节点上查看
ls /var/lib/kubelet/pods/d475ecaf-ce2e-4eac-af41-2d13bda6e0ec/volumes/
# kubernetes.io~configmap
# kubernetes.io~projected

tree /var/lib/kubelet/pods/.../volumes/
# ├── kubernetes.io~configmap
# │   └── config-volume
# │       └── redis.conf -> ..data/redis.conf    ← symlink
# └── kubernetes.io~projected
#     └── kube-api-access-w4f29
#         ├── ca.crt -> ../data/ca.crt
#         ├── namespace -> ../data/namespace
#         └── token -> ../data/token
```

`redis.conf` 实际是指向隐藏目录的 symlink：
```bash
ll -art /var/lib/kubelet/pods/.../volumes/kubernetes.io~configmap/config-volume/
# redis.conf -> ..data/redis.conf
# ..data -> ..2021_11_01_06_39_32.947090751/   ← 指向带时间戳的隐藏目录

ls ..2021_11_01_06_39_32.947090751/
# redis.conf    ← 真实文件
```

#### SetUpAt 源码（configmap.go:187）

```go
// pkg/volume/configmap/configmap.go:187
func (b *configMapVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
    // b.GetPath() 返回 /var/lib/kubelet/pods/{pid_uid}/kubernetes.io~configmap/{volume_name}

    // 1. 获取 ConfigMap（从 apiserver 或 informer cache）
    configMap, err := b.getConfigMap(b.pod.Namespace, b.source.Name)
    if err != nil {
        if !errors.IsNotFound(err) && !optional {
            return err
        }
        configMap = &v1.ConfigMap{
            ObjectMeta: metav1.ObjectMeta{
                Namespace: b.pod.Namespace,
                Name:      b.source.Name,
            },
        }
    }

    // 2. 从 ConfigMap 提取 payload（key→文件内容 map）
    payload, err := MakePayload(b.source.Items, configMap, b.source.DefaultMode, optional)
    if err != nil {
        return err
    }

    // 3. 创建挂载目录（/var/lib/kubelet/pods/{uid}/kubernetes.io~configmap/{vol_name}）
    if err := volumeutil.MakeNestedMountpoints(b.volName, dir, b.pod); err != nil {
        return err
    }

    // 4. 原子写文件（AtomicWriter：写隐藏目录 + 切换 symlink）
    writer, err := volumeutil.NewAtomicWriter(dir, fmt.Sprintf("pod %v", b.pod.UID))
    if err != nil {
        return err
    }
    err = writer.Write(payload)
    if err != nil {
        return err
    }
    // ...
}
```

`MakePayload`（:269）遍历 ConfigMap 的 `data` map，将每个 key-value 对转换为 `FileProjection{Data: []byte(value), Mode: mode}`。

`writer.Write` 的原子写逻辑：
1. 创建带时间戳的隐藏目录（`..2021_11_01_06_39_32.947090751/`）
2. 将所有文件写入该目录
3. 原子替换 `..data` symlink 指向新目录
4. 各文件名 symlink 指向 `..data/filename`

这样任何时刻读到的文件都是同一版本的完整配置，不会出现部分更新。

### 追踪 secretVolumeMounter 的 SetUp

源码路径：`pkg/volume/secret/secret.go`

```go
// secret.go:182
func (b *secretVolumeMounter) SetUpAt(dir string, mounterArgs volume.MounterArgs) error {
    // 结构与 configMapVolumeMounter.SetUpAt 完全一致
    // 区别：调用 b.getSecret(namespace, name) 而非 getConfigMap
    //       AtomicWriter.Write 写入同样格式的 payload
    err = writer.Write(payload)
    if err != nil {
        klog.Errorf("Error writing payload to dir: %v", err)
        return err
    }
}
```

secret 的底层挂载机制与 configmap 完全相同，都写到 `/var/lib/kubelet/pods/{pid_uid}/kubernetes.io~secret/` 目录，同样使用 AtomicWriter 的 symlink 原子切换。

### configMap 和 secret 热更新的问题

- 经过上面的源码分析，reconciler 协程处理挂载
- 所以热更新的流程：轮询检查 configmap 更新了就重新写入新的文件
- 热更新的频率取决于 reconciler 协程的轮询周期，也就是 `rc.loopSleepDuration` 的大小，**默认为 100 毫秒**

```go
// reconciler.go:145
func (rc *reconciler) Run(stopCh <-chan struct{}) {
    wait.Until(rc.reconciliationLoopFunc(), rc.loopSleepDuration, stopCh)
}

func (rc *reconciler) reconciliationLoopFunc() func() {
    return func() {
        rc.reconcile()
        // ...
    }
}
```

**热更新注意事项**：
- 以**环境变量**方式注入的 ConfigMap 值**不支持热更新**（环境变量在 Pod 启动时注入，之后不再变化）
- 只有以 **volume 挂载**方式使用的 ConfigMap/Secret 才支持热更新
- 热更新有最多约 2 分钟的延迟（reconciler 轮询 + apiserver cache TTL）
