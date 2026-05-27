# 第04章 自定义准入控制器，完成 nginx sidecar 的注入

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 04 章 — 自定义准入控制器，完成 nginx sidecar 的注入
> **源码入口**: 自研项目 `kube-mutating-webhook-inject-pod`（非 K8s 源码，本章为实战工程）

---

## 核心机制一览

1. **MutatingAdmissionWebhook 是 apiserver 的外挂钩子**：apiserver 收到 Pod create/update 请求后，会将 `AdmissionReview` 对象通过 HTTPS POST 发给外部 webhook server；webhook 返回 JSON Patch，apiserver 将 patch 应用到对象后再持久化。整个过程对调用方透明。

2. **webhook server 的核心接口只有一个：`/mutate`**：所有准入逻辑都在 `webhookServer.serveMutate` 方法里，HTTP 路由 → 请求校验 → `mutatePod` → `createPatch` → 返回 `AdmissionResponse` 是完整的处理链。

3. **annotation 驱动注入决策**：`mutationRequired` 函数通过三条规则判断是否注入：① 是否在系统保留命名空间（`kube-system`、`kube-public`）→ 跳过；② annotation `status` 已标记 `injected` → 跳过；③ annotation `need_inject` 为 `true` → 注入。注入与否完全由 Pod 的 annotation 控制，webhook server 本身无状态。

4. **JSON Patch 是修改对象的唯一手段**：webhook 不能直接修改 Pod 对象，只能返回一组 `patchOperation`（op/path/value）。容器注入用 `addContainer`，volume 注入用 `addVolume`，annotation 更新用 `updateAnnotation`，三者组合成最终 patch 字节。

5. **TLS 双向信任通过 K8s CSR 机制建立**：webhook server 持有由 apiserver CA 签名的证书（`server-cert.pem`），`MutatingWebhookConfiguration` 中的 `caBundle` 字段填入同一个 CA 证书；apiserver 发请求时用 caBundle 验证 webhook server 身份，从而建立信任。

6. **`namespaceSelector` 是 webhook 生效范围的开关**：`MutatingWebhookConfiguration` 通过 `namespaceSelector.matchLabels` 限定只对打了 `nginx-sidecar-injection: enabled` 标签的命名空间生效；没打标签的 namespace（如 `default`、`kube-system`）里的 Pod 创建不会触发 webhook。

---

## 全章调用链总图

```
kubectl create pod (annotations: need_inject=true)
  │
  ▼ apiserver 收到请求
  │   ├── Authentication → Authorization → Admission
  │   └── Admission: MutatingAdmissionWebhook
  │         │ 查 MutatingWebhookConfiguration
  │         │   namespaceSelector: nginx-sidecar-injection=enabled
  │         │   rules: CREATE/UPDATE pods
  │         │   clientConfig.service: sidecar-injector-webhook-svc/mutate
  │         ▼
  ▼ HTTPS POST → sidecar-injector-webhook-svc:443/mutate       webhook.go
  │   └── serveMutate(w, r)
  │         ├── 1. 读 body，校验 Content-Type = application/json
  │         ├── 2. UniversalDeserializer.Decode → AdmissionReview
  │         ├── 3. mutatePod(ar)
  │         │     ├── json.Unmarshal → corev1.Pod
  │         │     ├── mutationRequired(ignoredNamespaces, pod.ObjectMeta)
  │         │     │     ├── 在 kube-system/kube-public？→ skip
  │         │     │     ├── status == "injected"？→ skip
  │         │     │     └── need_inject == "true"？→ inject
  │         │     └── createPatch(pod, sidecarConfig, annotations)
  │         │           ├── addContainer(existing, sidecar, "/spec/containers")
  │         │           ├── addVolume(existing, volumes, "/spec/volumes")
  │         │           └── updateAnnotation(existing, {status: "injected"})
  │         └── 4. AdmissionResponse{Allowed:true, Patch:patchBytes, PatchType:JSONPatch}
  │               └── json.Marshal → w.Write
  │
  ▼ apiserver 应用 patch → Pod 含 nginx sidecar → 写入 etcd
```

---

## §01 自定义准入控制器需求分析

本节说明本章实战目标：仿照 Istio 自动注入 Envoy sidecar 的思路，实现一个 MutatingAdmissionWebhook，在 Pod 创建时自动注入 nginx sidecar 容器。

本章的设计思路来自 Istio：Istio 用同一套 MutatingAdmissionWebhook 机制在每个 Pod 旁注入 Envoy proxy sidecar，本章用同样的机制注入 nginx sidecar 作为学习实现。

### 本章目标流程

```
检查集群是否启用 admission webhook
  │
  ▼ 编写 mutating webhook 代码（pkg/webhook.go）
  │   ├── 启动 TLS HTTPS server
  │   ├── 实现 /mutate 方法：Create/Update Pod 时触发
  │   │     └── apiserver 调用此 webhook，修改 spec，添加 nginx sidecar
  │   └── 返回给 apiserver，打到注入的目的
  │
  ▼ 创建证书，完成 CA 签名
  │
  ▼ 创建 MutatingWebhookConfiguration
  │
  ▼ 部署服务验证注入结果
```

---

## §02 检查集群准入配置和准备工作

### 检查集群是否启用了准入注册 API

```bash
kubectl api-versions | grep admission
# admissionregistration.k8s.io/v1
# admissionregistration.k8s.io/v1beta1
```

`admissionregistration.k8s.io/v1` 存在即表示集群已启用 `MutatingAdmissionWebhook` 和 `ValidatingAdmissionWebhook` 两个准入控制器。kube-apiserver 的 `--enable-admission-plugins` 参数默认已包含这两项。

### 新建项目

```bash
go mod init kube-mutating-webhook-inject-pod
```

### sidecar 容器配置文件设计（config.yaml）

注入一个容器需要同时携带它的 `container` 定义和挂载的 `volumes`，因此配置文件同时复用 K8s Pod spec 中的 `containers` 和 `volumes` 字段格式：

```yaml
containers:
  - name: sidecar-nginx
    image: nginx:1.12.2
    imagePullPolicy: IfNotPresent
    ports:
      - containerPort: 80
    volumeMounts:
      - name: nginx-conf
        mountPath: /etc/nginx
volumes:
  - name: nginx-conf
    configMap:
      name: nginx-configmap
```

### Config 结构体（pkg/webhook.go）

```go
// pkg/webhook.go
import corev1 "k8s.io/api/core/v1"

type Config struct {
    Containers []corev1.Container `yaml:"containers"`
    Volumes    []corev1.Volume    `yaml:"volumes"`
}
```

直接复用 `corev1.Container` / `corev1.Volume`，可以用标准 K8s YAML 格式写 sidecar 配置，避免重复定义结构。

### loadConfig：解析配置文件

```go
func loadConfig(configFile string) (*Config, error) {
    data, err := ioutil.ReadFile(configFile)
    if err != nil {
        return nil, err
    }
    glog.Infof("New configuration: sha256sum %x", sha256.Sum256(data))

    var cfg Config
    if err := yaml.Unmarshal(data, &cfg); err != nil {
        return nil, err
    }
    return &cfg, nil
}
```

### WebhookServerOptions 结构体

```go
// Webhook Server options
type WebhookServerOptions struct {
    port           int    // 监听 HTTPS 端口
    certFile       string // HTTPS server 证书路径
    keyFile        string // HTTPS 私钥路径
    sidecarCfgFile string // sidecar 注入容器的配置文件路径
}
```

### main() 框架

```go
func main() {
    // 解析命令行参数
    flag.IntVar(&runOption.port, "port", 8443, "webhook server port.")
    flag.StringVar(&runOption.certFile, "tlsCertFile", "/etc/webhook/certs/cert.pem", "...")
    flag.StringVar(&runOption.keyFile, "tlsKeyFile", "/etc/webhook/certs/key.pem", "...")
    flag.StringVar(&runOption.sidecarCfgFile, "sidecarCfgFile", "/etc/webhook/config/config.yaml",
        "File containing the mutation configuration.")
    flag.Parse()

    // 加载 sidecar 配置
    sidecarConfig, err := loadConfig(runOption.sidecarCfgFile)
    if err != nil {
        glog.Errorf("Failed to load configuration: %v", err)
        return
    }

    // 加载 TLS 证书
    pair, err := tls.LoadX509KeyPair(runOption.certFile, runOption.keyFile)
    if err != nil {
        glog.Errorf("Failed to load key pair: %v", err)
    }

    // 构造 webhookServer
    webhooksvr := &webhookServer{
        sidecarConfig: sidecarConfig,
        server: &http.Server{
            Addr:      fmt.Sprintf(":%v", runOption.port),
            TLSConfig: &tls.Config{Certificates: []tls.Certificate{pair}},
        },
    }

    // 注册路由
    mux := http.NewServeMux()
    mux.HandleFunc("/mutate", webhooksvr.serveMutate)
    webhooksvr.server.Handler = mux

    // 在新 goroutine 中启动 HTTPS server
    go func() {
        if err := webhooksvr.server.ListenAndServeTLS("", ""); err != nil {
            glog.Errorf("Failed to listen and serve webhook server: %v", err)
        }
    }()

    // 监听退出信号
    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
    <-signalChan
    glog.Infof("Got OS shutdown signal, shutting down webhook server gracefully...")
    webhooksvr.server.Shutdown(context.Background())
}
```

设计要点：
- `webhookServer` 持有 `sidecarConfig`（注入什么容器）和 `*http.Server`（监听谁）；HTTPS 证书在构造时一次性注入到 `TLSConfig`。
- server 在独立 goroutine 运行，main goroutine 阻塞在信号 channel 上，收到 SIGTERM/SIGINT 后调用 `Shutdown` 优雅退出。

---

## §03 注入 sidecar 的 mutatePod 注入函数编写

```
serveMutate(w, r)
  │
  ├── 1. 请求校验（body 非空、Content-Type = application/json）
  ├── 2. UniversalDeserializer.Decode → AdmissionReview
  ├── 3. mutatePod(ar)
  │     ├── json.Unmarshal(ar.Request.Object.Raw, &pod)
  │     ├── mutationRequired(ignoredNamespaces, pod.ObjectMeta)
  │     └── createPatch(pod, ws.sidecarConfig, annotations)
  └── 4. 构造 AdmissionReview{Response} → json.Marshal → w.Write
```

本节从 `serveMutate` 出发，深入到 `mutatePod` 和 `createPatch` 的完整实现。

### serveMutate：请求校验

```go
func (ws *webhookServer) serveMutate(w http.ResponseWriter, r *http.Request) {
    var body []byte
    if r.Body != nil {
        if data, err := ioutil.ReadAll(r.Body); err == nil {
            body = data
        }
    }
    if len(body) == 0 {
        glog.Error("empty body")
        http.Error(w, "empty body", http.StatusBadRequest)
        return
    }
    contentType := r.Header.Get("Content-Type")
    if contentType != "application/json" {
        glog.Errorf("Content-Type=%s, expect application/json", contentType)
        http.Error(w, "invalid Content-Type, expect `application/json`", http.StatusUnsupportedMediaType)
        return
    }
    // ...
}
```

### 解析 AdmissionReview：UniversalDeserializer

```go
var (
    runtimeScheme = runtime.NewScheme()
    codecs        = serializer.NewCodecFactory(runtimeScheme)
    deserializer  = codecs.UniversalDeserializer()
    // 为 Pod ObjectMeta 填充默认值（来自 k8s PR #58025）
    defaulter     = runtime.ObjectDefaulter(runtimeScheme)
)

// serveMutate 中：
var admissionResponse *v1beta1.AdmissionResponse
ar := v1beta1.AdmissionReview{}
if _, _, err := deserializer.Decode(body, nil, &ar); err != nil {
    glog.Errorf("Can't decode body: %v", err)
    admissionResponse = &v1beta1.AdmissionResponse{
        Result: &metav1.Status{Message: err.Error()},
    }
} else {
    admissionResponse = ws.mutatePod(&ar)
}
```

`UniversalDeserializer` 来自 `k8s.io/apimachinery/pkg/runtime/serializer/codec_factory.go`，能自动识别 API version/kind 并反序列化为对应 Go 对象，比 `json.Unmarshal` 更安全（会做 scheme 校验）。

### 写入响应

```go
admissionReview := v1beta1.AdmissionReview{}
if admissionResponse != nil {
    admissionReview.Response = admissionResponse
    if ar.Request != nil {
        admissionReview.Response.UID = ar.Request.UID  // UID 必须回写，apiserver 靠它匹配请求
    }
}
resp, err := json.Marshal(admissionReview)
if err != nil {
    http.Error(w, fmt.Sprintf("could not encode response: %v", err), http.StatusInternalServerError)
    return
}
w.Write(resp)
```

### mutatePod：解析 Pod + 注入判断

```go
func (ws *webhookServer) mutatePod(ar *v1beta1.AdmissionReview) *v1beta1.AdmissionResponse {
    req := ar.Request
    var pod corev1.Pod
    if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
        glog.Errorf("Could not unmarshal raw object: %v", err)
        return &v1beta1.AdmissionResponse{
            Result: &metav1.Status{Message: err.Error()},
        }
    }

    // 判断是否需要注入
    if !mutationRequired(ignoredNamespaces, &pod.ObjectMeta) {
        glog.Infof("Skipping mutation for %s/%s due to policy check", pod.Namespace, pod.Name)
        return &v1beta1.AdmissionResponse{Allowed: true}
    }

    // 为 Pod 填充默认值（解决 objectMeta 空字段问题）
    applyDefaultsWorkaround(ws.sidecarConfig.Containers, ws.sidecarConfig.Volumes)

    // 生成注入 annotation：status=injected
    annotations := map[string]string{admissionWebhookAnnotationStatusKey: "injected"}
    patchBytes, err := createPatch(&pod, ws.sidecarConfig, annotations)
    if err != nil {
        return &v1beta1.AdmissionResponse{
            Result: &metav1.Status{Message: err.Error()},
        }
    }

    glog.Infof("AdmissionResponse: patch=%v\n", string(patchBytes))
    return &v1beta1.AdmissionResponse{
        Allowed:   true,
        Patch:     patchBytes,
        PatchType: func() *v1beta1.PatchType {
            pt := v1beta1.PatchTypeJSONPatch
            return &pt
        }(),
    }
}
```

### mutationRequired：三条跳过规则

```go
// 常量：annotation key
const (
    admissionWebhookAnnotationInjectKey = "sidecar-injector-webhook.nginx.sidecar/need_inject"
    admissionWebhookAnnotationStatusKey = "sidecar-injector-webhook.nginx.sidecar/status"
)

// 系统命名空间不注入
var ignoredNamespaces = []string{
    metav1.NamespaceSystem, // kube-system
    metav1.NamespacePublic, // kube-public
}

func mutationRequired(ignoredList []string, metadata *metav1.ObjectMeta) bool {
    // 规则1：在系统保留 namespace 中 → 不注入
    for _, namespace := range ignoredList {
        if metadata.Namespace == namespace {
            glog.Infof("Skip mutation for %v in special namespace:%v", metadata.Name, metadata.Namespace)
            return false
        }
    }

    annotations := metadata.GetAnnotations()
    if annotations == nil {
        annotations = map[string]string{}
    }

    // 规则2：已经注入过（status=injected）→ 不注入
    status := annotations[admissionWebhookAnnotationStatusKey]
    if strings.ToLower(status) == "injected" {
        return false
    }

    // 规则3：need_inject 必须显式为 "true" 才注入，其他值（false/缺省）均跳过
    switch strings.ToLower(annotations[admissionWebhookAnnotationInjectKey]) {
    case "true":
        return true   // need_inject=true → 需要注入
    default:
        return false  // 缺省/false → 不注入
    }
}
```

三条规则的设计逻辑：通过 annotation 而非 webhook server 内部状态驱动，Pod 自带注入历史（`status=injected`），重启 webhook server 后不会重复注入。

### applyDefaultsWorkaround

```go
func applyDefaultsWorkaround(containers []corev1.Container, volumes []corev1.Volume) {
    defaulter.Default(&corev1.Pod{
        Spec: corev1.PodSpec{
            Containers: containers,
            Volumes:    volumes,
        },
    })
}
```

来自 [kubernetes/kubernetes#58025](https://github.com/kubernetes/kubernetes/pull/58025)：在生成 patch 前先对 sidecar containers 填充 K8s 默认值（如 `imagePullPolicy` 的默认值），防止 patch apply 后触发 schema 校验失败。

### createPatch：组合三类 patch

```go
type patchOperation struct {
    Op    string      `json:"op"`              // 动作：add / replace
    Path  string      `json:"path"`            // 操作路径（JSON Pointer）
    Value interface{} `json:"value,omitempty"` // 值
}

func createPatch(pod *corev1.Pod, sidecarConfig *Config, annotations map[string]string) ([]byte, error) {
    var patch []patchOperation

    patch = append(patch, addContainer(
        pod.Spec.Containers, sidecarConfig.Containers, "/spec/containers")...)
    patch = append(patch, addVolume(
        pod.Spec.Volumes, sidecarConfig.Volumes, "/spec/volumes")...)
    patch = append(patch, updateAnnotation(
        pod.Annotations, annotations)...)

    return json.Marshal(patch)
}
```

### addContainer / addVolume：首元素特殊处理

```go
func addContainer(target, added []corev1.Container, basePath string) (patch []patchOperation) {
    first := len(target) == 0
    var value interface{}
    for _, add := range added {
        value = add
        path := basePath
        if first {
            first = false
            value = []corev1.Container{add} // 首次：整个数组
        } else {
            path = path + "/-"              // 追加：用 /- 表示数组末尾
        }
        patch = append(patch, patchOperation{Op: "add", Path: path, Value: value})
    }
    return patch
}
```

JSON Patch 规范：`/spec/containers` 表示替换整个数组；`/spec/containers/-` 表示在数组末尾追加元素。当 Pod 已有容器时用 `/-`，当 Pod 没有任何容器时用根路径并把 value 包装成 `[]Container`。`addVolume` 逻辑完全对称。

### updateAnnotation：add vs replace

```go
func updateAnnotation(target map[string]string, added map[string]string) (patch []patchOperation) {
    for key, value := range added {
        if target == nil || target[key] == "" {
            // annotations 不存在或 key 不存在 → add 整个 map
            target = map[string]string{}
            patch = append(patch, patchOperation{
                Op:    "add",
                Path:  "/metadata/annotations",
                Value: map[string]string{key: value},
            })
        } else {
            // key 已存在 → replace 对应 key
            patch = append(patch, patchOperation{
                Op:    "replace",
                Path:  "/metadata/annotations/" + key,
                Value: value,
            })
        }
    }
    return patch
}
```

annotation key 含 `/` 时需要 JSON Pointer 转义（`/` → `~1`，`~` → `~0`），此处 key 不含特殊字符所以可以直接拼接。

---

## §04 打镜像部署并运行注入 sidecar 验证

### 编译与打镜像

两个关键编译决策：① `CGO_ENABLED=0` 静态编译，产物可直接放进 `alpine:latest`（无 glibc 依赖）；② `GOOS=linux GOARCH=amd64` 交叉编译，确保在 Windows/Mac 开发机上也能构建 Linux 镜像。

```bash
make build-image   # build-linux → docker build -f Dockerfile .
```

Dockerfile 以非 root 用户（UID=1001）运行二进制，符合安全最佳实践。

containerd 环境导入镜像：

```bash
docker save sidecar-injector > a.tar && scp a.tar k8s-node01:~
ctr --namespace k8s.io images import a.tar
```

### 创建 CA 证书，并让 apiserver 签名

webhook server 必须使用由集群 CA 签名的证书，apiserver 才会信任它。利用 K8s 内置的 `CertificateSigningRequest` 机制，不需要外部 CA。

**核心设计：为什么用 K8s CSR 而不是自签 CA**

webhook server 必须持有由集群 CA 签名的证书，`MutatingWebhookConfiguration.caBundle` 填同一个 CA，apiserver 用它验证 webhook server 身份。利用 K8s 内置的 `CertificateSigningRequest` 资源可以直接向集群 CA 申请签名，不需要自己维护 CA。

**SAN 必须覆盖所有 DNS 名：**

```
DNS.1 = sidecar-injector-webhook-svc
DNS.2 = sidecar-injector-webhook-svc.sidecar-injector
DNS.3 = sidecar-injector-webhook-svc.sidecar-injector.svc
```

apiserver 通过 Service DNS 访问 webhook，TLS 验证时会对比请求的 hostname 与证书 SAN，三个 DNS 名对应不同 namespace 解析深度，缺少任何一个都会导致 TLS handshake 失败。

**证书流程（5 步）：**

```
openssl genrsa → server-key.pem（私钥）
openssl req -new → server.csr（证书请求，含 SAN）
kubectl create CertificateSigningRequest → 提交给集群 CA
kubectl certificate approve → 审批
kubectl get csr -o jsonpath='.status.certificate' → 取回签名证书 server-cert.pem
kubectl create secret generic sidecar-injector-webhook-certs → 存为 Secret，挂载进 Pod
```

### 部署 yaml

**部署顺序：**

```
01  kubectl create -f deploy/inject_{configmap,deployment,service}.yaml
    → 启动 webhook server（ClusterIP Service，port 443）

02  CA_BUNDLE = 从 kubeconfig 的 certificate-authority-data 提取
    → sed 替换 mutating_webhook.yaml 中的 ${CA_BUNDLE} 占位符
    → kubectl create -f deploy/mutatingwebhook-ca-bundle.yaml

03  kubectl create -f deploy/nginx_configmap.yaml
    → sidecar 所需的 nginx 配置
```

`CA_BUNDLE` 优先从 kubeconfig 的 `clusters[].cluster.certificate-authority-data` 提取；若 kubeconfig 不含内嵌 CA（指向文件），则从 `default` ServiceAccount 的 secret 中读取 `ca.crt`。两种来源是同一个集群 CA 证书，最终填入 `MutatingWebhookConfiguration.webhooks[].clientConfig.caBundle`。

### MutatingWebhookConfiguration 关键字段

```yaml
apiVersion: admissionregistration.k8s.io/v1beta1
kind: MutatingWebhookConfiguration
metadata:
  name: sidecar-injector-webhook-cfg
  labels:
    app: sidecar-injector
webhooks:
  - name: sidecar-injector-webhook.nginx.sidecar
    clientConfig:
      service:
        name: sidecar-injector-webhook-svc
        namespace: sidecar-injector
        path: "/mutate"
      caBundle: ${CA_BUNDLE}          # apiserver 用它验证 webhook server 证书
    rules:
      - operations: ["CREATE", "UPDATE"]
        apiGroups: [""]
        apiVersions: ["v1"]
        resources: ["pods"]
    namespaceSelector:
      matchLabels:
        nginx-sidecar-injection: enabled  # 只对打了此标签的 ns 生效
```

`namespaceSelector` 是安全边界：未打标签的 namespace（`default`、`kube-system` 等）里的 Pod 创建不会触发此 webhook，防止误注入影响系统组件。

### 创建目标 namespace 并打标签

```bash
kubectl create ns nginx-injection
kubectl label namespace nginx-injection nginx-sidecar-injection=enabled
```

```bash
# 验证标签
kubectl get ns -L nginx-sidecar-injection
# nginx-injection  Active  NGINX-SIDECAR-INJECTION=enabled
```

### 验证注入

**部署 05：打了 `need_inject: "true"` 的 Pod**

```yaml
apiVersion: v1
kind: Pod
metadata:
  namespace: nginx-injection
  name: test-alpine-inject-01
  annotations:
    sidecar-injector-webhook.nginx.sidecar/need_inject: "true"
spec:
  containers:
    - image: alpine
      command: ["/bin/sh", "-c", "sleep 60m"]
      imagePullPolicy: IfNotPresent
      name: alpine
      restartPolicy: Always
```

```bash
kubectl create -f test_sleep_deployment.yaml
kubectl get pod -n nginx-injection -o wide
# test-alpine-inject-01   2/2   Running   ...   10.100.85.216   k8s-node01
# READY=2/2 说明有两个容器（alpine + nginx sidecar）

curl 10.100.85.216   # 访问 Pod IP 的 80 端口
# <title>404 Not Found</title>  ← nginx 响应，证明 sidecar 已注入
```

**webhook server 日志（注入成功）：**

```
I0910 08:35:14.788857  1  webhook.go:179  serveMutate.receive.request: Body={"kind":"AdmissionReview"...
I0910 08:35:14.789561  1  webhook.go:128  AdmissionReview for Kind=v1, Kind=Pod, Namespace=nginx-injection
I0910 08:35:14.789844  1  webhook.go:260  [addContainer.value][add:+v][value:+v] %!(EXTRA v1.Container=...
I0910 08:35:14.789945  1  webhook.go:154  AdmissionResponse: patch=[{"op":"add","path":"/spec/...
I0910 08:35:14.789987  1  webhook.go:221  Ready to write response ...
```

**部署 07：打了 `need_inject: "false"` 的 Pod（验证跳过逻辑）**

```yaml
annotations:
  sidecar-injector-webhook.nginx.sidecar/need_inject: "false"
```

```bash
kubectl get pod -n nginx-injection -o wide
# test-alpine-inject-02   1/1   Running   ...   # READY=1/1，只有一个容器
```

**webhook server 日志（跳过注入）：**

```
I0910 08:40:05.466685  1  webhook.go:128  AdmissionReview for Kind=v1, Kind=Pod, Namespace=nginx-injection
I0910 08:40:05.466733  1  webhook.go:106  [skip_mutation][reason=pod_not_need][name:test-alpine-inject-02]
I0910 08:40:05.466765  1  webhook.go:133  Skipping mutation for nginx-injection/test-alpine-inject-02
I0910 08:40:05.466815  1  webhook.go:221  Ready to write response ...
```
