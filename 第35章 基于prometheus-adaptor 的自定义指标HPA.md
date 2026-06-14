# 第35章 基于 prometheus-adapter 的自定义指标 HPA

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 35 章 — 基于 prometheus-adapter 的自定义指标 HPA
> **源码入口**: `kube-aggregator`（第34章已覆盖），本章为实战部署篇，无新增源码入口

---

## 核心机制一览

1. **自定义指标 HPA 的完整链路**：业务 Pod 用 prometheus SDK 暴露 `/metrics` → Prometheus 采集 → prometheus-adapter 将指标转换并注册到 `custom.metrics.k8s.io` API → HPA 从该 API 查询指标 → 触发扩缩容。整条链路无需修改 HPA 控制器源码，全靠 kube-aggregator 的 API 扩展机制实现。

2. **prometheus-adapter rule 的核心字段**：`seriesQuery` 指定从 Prometheus 查询哪个指标；`name.matches`/`name.as` 控制指标改名（如 `*_total` → `*_per_second`）；`metricsQuery` 是真正发到 Prometheus 的 PromQL（通常用 `rate()` 将 counter 转为 per-second 速率）；`resources.overrides` 将 Prometheus 标签映射到 Kubernetes 资源维度（namespace/pod）。

3. **counter 指标必须用 rate() 转换**：业务 SDK 暴露的登录计数是 counter 类型（只增不减），HPA 需要的是当前速率（每秒请求数）。prometheus-adapter 的 `metricsQuery` 用 `sum(rate(<<.Series>>{<<.LabelMatchers>>}[1m])) by (<<.GroupBy>>)` 完成这一转换，`<<.GroupBy>>` 按 Pod 维度聚合。

4. **HPA targetAverageValue 语义**：HPA spec 中 `targetAverageValue: 10` 表示每个 Pod 平均每秒处理10个请求。当实测 QPS 超过 `replicas × 10` 时触发扩容，计算公式与第34章 `ceil(currentReplicas × currentUtilization / targetUtilization)` 一致。

5. **prometheus-adapter 注册为 APIService**：部署后，`/apis/custom.metrics.k8s.io` 端点由 prometheus-adapter 的 Pod 处理，通过 kube-aggregator 代理。可用 `kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1"` 验证注册状态，用 `kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/..."` 查询具体指标值。

---

## §01 部署 prometheus-adapter

### 整体架构

本章部署三个组件，形成完整的自定义指标 HPA 链路：

```
业务 Pod (login-pod)          Prometheus              prometheus-adapter
  /login   → myReq.Inc()  →  采集 /metrics  →  注册到 custom.metrics.k8s.io
  /metrics → promhttp      →  存储指标数据  →  HPA 查询触发扩容
```

### 手动部署 prometheus-adapter

**第一步：创建证书（gencerts.sh）**

prometheus-adapter 与 kube-apiserver 之间走 HTTPS，需要 TLS 证书。

```bash
# clone 项目
git clone https://github.com/kubernetes-sigs/prometheus-adapter.git

# 下载生成证书/configMap YAML 的部署脚本
wget https://github.com/prometheus-operator/kube-prometheus/raw/62fff622e9900fade8aecbd02bc9c557b...

# 修改 gencerts.sh：添加 GOSUM DB=off，跳过 go 包 sum 校验，避免 SECURITY ERROR
#!/usr/bin/env bash
# ...
GONOSUMDB=* go get -v github.com/cloudflare/cfssl/cmd/...

export PURPOSE=...
openssl req -x509 -new -nodes -days 365 -newkey rsa:2048 ...
echo "[$(date)] Signing ..."
cat > "${ALT_NAMES}" << EOF
[ v3_ca ]
subjectAltName = "DNS:custom-metrics-apiserver, DNS:custom-metrics-apiserver.custom-metrics, ..."
EOF

export SERVICE_NAME=custom-metrics-apiserver
export ALT_NAMES=...
export AUTH_DELEGATOR_ARGS=...
echo "---
apiVersion: v1
kind: Secret
..." > cm-adapter-serving-certs.yaml
```

**第二步：创建 namespace 并生成证书**

```bash
kubectl create namespace custom-metrics
# 生成证书以及 prometheus-adapter 所需要的 ConfigMap
sh gencerts.sh
# 执行后在当前目录生成 cm-adapter-serving-certs.yaml
```

生成的文件清单包括：`hpa-custom-metrics-cluster-role-binding.yaml`、`custom-metrics-resource-reader-cluster-role.yaml`、`custom-metrics-apiserver-deployment.yaml`、`custom-metrics-apiserver-service.yaml`、`custom-metrics-apiserver-service-account.yaml`、`metrics-ca.key`、`metrics.key`、`metrics-ca.crt`、`metrics.crt`、`adapter.crt`、`adapter.key`、`prometheus.yaml` 等。

**第三步：部署 prometheus（简化版）**

本章只需要一个简单的 prometheus 实例，所以 prometheus 存储的高可用等复杂配置统统不需要，只需要准备 rbac、configmap 和一个 sts。

RBAC（ClusterRole + ServiceAccount + ClusterRoleBinding）：

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: prometheus-single
rules:
- apiGroups: [""]
  resources:
  - nodes
  - nodes/metrics
  - nodes/proxy
  - services
  - endpoints
  - pods
  verbs: ["get", "list", "watch"]
- apiGroups:
  - extensions
  resources:
  - ingresses
  verbs: ["get", "list", "watch"]
- nonResourceURLs: ["/metrics"]
  verbs: ["get"]
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: prometheus-single
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: prometheus-single
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: prometheus-single
subjects:
- kind: ServiceAccount
  name: prometheus-single
  namespace: default
```

ConfigMap（只保留 apiserver、pod 和 cadvisor 的采集）：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
data:
  prometheus.yml: |
    global:
      scrape_interval: 30s
      evaluation_interval: 30s
      external_labels:
        cluster: "01"
    scrape_configs:
    - job_name: 'kubernetes-apiservers'
      kubernetes_sd_configs:
      - role: endpoints
      scheme: https
      tls_config:
        ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      bearer_token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
      relabel_configs:
      - source_labels: [__meta_kubernetes_namespace, __meta_kubernetes_service_name, ...]
        action: keep
        regex: default;kubernetes;https
    - job_name: kube-state-metrics
      metrics_path: /metrics
      scheme: http
      static_configs:
      ...
```

StatefulSet（prometheus 容器 + configmap-reload 热更新 sidecar）：

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: prometheus-single
spec:
  serviceName: "prometheus-single"
  podManagementPolicy: "Parallel"
  replicas: 1
  template:
    spec:
      containers:
      - name: prometheus-server-configmap-reload
        image: "jimmidyson/configmap-reload:v0.4.0"
        args:
        - --volume-dir=/etc/config
        - --webhook-url=http://localhost:9090/-/reload
        volumeMounts:
        - name: config-volume
          mountPath: /etc/config
          readOnly: true
        resources:
          limits:
            cpu: 10m
            memory: 10Mi
      - image: prom/prometheus:v2.29.1
        name: prometheus
        command:
        - "/bin/prometheus"
        args:
        - "--config.file=/etc/prometheus/prometheus.yml"
        - "--storage.tsdb.path=/prometheus"
        - "--storage.tsdb.retention=24h"
        - "--web.console.libraries=/etc/prometheus/console_libraries"
        - "--web.console.templates=/etc/prometheus/consoles"
        - "--web.enable-lifecycle"
        - "--web.listen-address=0.0.0.0:9090"
        ports:
        - containerPort: 9090
          protocol: TCP
        volumeMounts:
        - mountPath: "/prometheus"
          name: prometheus-data
        - mountPath: "/etc/prometheus"
          name: config-volume
        readinessProbe:
          httpGet:
            path: /-/ready
            port: 9090
          initialDelaySeconds: 3
          timeoutSeconds: 5
        livenessProbe:
          httpGet:
            path: /-/healthy
            port: 9090
          initialDelaySeconds: 3
          timeoutSeconds: 5
        resources:
          requests:
            cpu: 100m
            memory: 100Mi
          limits:
            cpu: 1000m
            memory: 1500Mi
        securityContext:
          runAsUser: 65534
```

Service（NodePort 供外部访问 Prometheus UI）：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: prometheus-single
spec:
  type: NodePort
  ports:
  - port: 9090
    targetPort: 9090
    protocol: TCP
  selector:
    name: prometheus-single
```

**第四步：部署 prometheus-adapter**

修改 `custom-metrics-apiserver-deployment.yaml`，填入 prometheus 的 svc 地址：

```bash
vim custom-metrics-apiserver-deployment.yaml
# prometheus-url 修改为：
prometheus-url=http://prometheus-single.default.svc:9090/
# 镜像地址修改为：
registry.cn-beijing.aliyuncs.com/ning1875_k8s_image/prometheus-adapter:v0.9.0
```

用之前生成的证书创建 Secret 并部署：

```bash
kubectl apply -f cm-adapter-serving-certs.yaml -n custom-metrics
kubectl apply -f .   # 部署所有 yaml
```

部署完成后可以看到在 custom-metrics ns 下创建了 svc 和 deployment。

**验证注册**

```bash
# 验证 custom.metrics.k8s.io API Group 已注册
kubectl get --raw "/apis/custom.metrics.k8s.io" | python -m json.tool
```

返回结果：

```json
{
  "apiVersion": "v1",
  "kind": "APIGroup",
  "name": "custom.metrics.k8s.io",
  "preferredVersion": {
    "groupVersion": "custom.metrics.k8s.io/v1beta2",
    "version": "v1beta2"
  },
  "versions": [
    {"groupVersion": "custom.metrics.k8s.io/v1beta2", "version": "v1beta2"},
    {"groupVersion": "custom.metrics.k8s.io/v1beta1", "version": "v1beta1"}
  ]
}
```

---

## §02 golang 程序统计登录 QPS

### 目标

实现一个暴露 `/login` 和 `/metrics` 接口的 Go HTTP 服务，每次 `/login` 请求将 prometheus counter 指标 +1，prometheus 采集 `/metrics` 后 adapter 将其转为 per-second 速率，供 HPA 查询。

### Go 代码

**指标定义与注册**

```go
package main

import (
    "fmt"
    "net/http"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    myReq = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "my_golang_app_http_req_login_total",  // counter 类型，只增不减
        Help: "k8s node detail each",
    })
)

func init() {
    prometheus.DefaultRegisterer.MustRegister(myReq)
}

func HelloHandler(w http.ResponseWriter, r *http.Request) {
    myReq.Inc()                    // 每次 /login 请求 counter +1
    fmt.Fprintf(w, "Hello World")
}

func main() {
    http.HandleFunc("/login", HelloHandler)
    http.Handle("/metrics", promhttp.Handler())   // prometheus SDK 标准 handler
    http.ListenAndServe(":8000", nil)
}
```

### Dockerfile

多阶段构建：golang:1.16-alpine 编译，yaruritux/busybox-curl 作为运行镜像：

```dockerfile
FROM golang:1.16-alpine as builder
WORKDIR /usr/src/app
ENV GOPROXY=https://goproxy.cn
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
    apk add --no-cache upx ca-certificates tzdata
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o login-pod

FROM yaruritux/busybox-curl as runner
COPY --from=builder /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/src/app/login-pod /opt/app/login-pod
ENTRYPOINT [ "/opt/app/login-pod" ]
```

### Deployment

关键点：Pod template 的 annotations 必须有 `prometheus.io/scrape: 'true'`，这样 prometheus 的 `kubernetes-pods` job 才会自动发现并采集该 Pod 的 `/metrics`：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: login-pod-deployment
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: login-pod-deployment
  template:
    metadata:
      labels:
        app: login-pod-deployment
      annotations:
        prometheus.io/scrape: 'true'    # 告诉 prometheus 采集此 pod
        prometheus.io/port: '8000'
        prometheus.io/path: 'metrics'
    spec:
      containers:
      - name: login-pod
        image: login-pod:v1
        command:
        - /opt/app/login-pod
        ports:
        - containerPort: 8000
        resources:
          requests:
            cpu: 100m
            memory: 100Mi
          limits:
            cpu: 200m
            memory: 200Mi
```

### 本地构建与导入

```bash
# 本地 build 并传输到远端机器
docker build -t login-pod:v1 . && docker save login-pod > login-pod.tar && scp login-pod.tar k8s-node01:~

# 导入镜像
docker load < login-pod.tar          # docker runtime
ctr --namespace k8s.io images import login-pod.tar  # containerd runtime

# 部署并验证
kubectl apply -f deployment.yaml
kubectl get pod -l app=login-pod-deployment -o wide
```

### 验证指标暴露

```bash
# 从 pod ip 访问 /metrics
curl 10.100.85.255:8000/login
curl 10.100.85.255:8000/metrics | grep my_golang_app_http_req_login_total
# 输出示例：
# TYPE my_golang_app_http_req_login_total counter
# my_golang_app_http_req_login_total{} 5
```

### 在 prometheus 查询指标

由于 login-pod deployment 的 annotations 中配置了 prometheus 采集信息，prometheus 的 `kubernetes-pods` job 会自动发现该 Pod 并采集。在 prometheus 中查询：

```
my_golang_app_http_req_login_total{app="login-pod-deployment", instance="10.100.85.255:8000", job="kubernetes-pods"}
```

也可以部署 Service 通过 cluster IP 访问 `/metrics`：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: login-pod-svc
spec:
  selector:
    app: login-pod-deployment
  ports:
  - protocol: TCP
    port: 8000
    targetPort: 8000
```

---

## §03 基于 prometheus-adapter 的自定义指标扩容

### 编写 prometheus-adapter rule

编辑 `custom-metrics-config-map.yaml`，添加指标转换规则：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: adapter-config
  namespace: custom-metrics
data:
  config.yaml: |
    rules:
    - seriesQuery: 'my_golang_app_http_req_login_total'
      resources:
        overrides:
          kubernetes_namespace:
            resource: namespace
          kubernetes_pod_name:
            resource: pod
      name:
        matches: "^(.*)_total"
        as: "${1}_per_second"          # my_golang_app_http_req_login_per_second
      metricsQuery: (sum(rate(<<.Series>>{<<.LabelMatchers>>}[1m])) by (<<.GroupBy>>))
```

**规则字段解读：**

| 字段 | 作用 |
|------|------|
| `seriesQuery` | 从 Prometheus 查询的指标名，所有匹配的时序都可以用于 HPA |
| `seriesFilters` | 进一步过滤，不需要的指标可以过滤掉 |
| `resources.overrides` | 将 Prometheus 标签映射到 K8s 资源维度；`kubernetes_namespace` → namespace 维度，`kubernetes_pod_name` → pod 维度 |
| `name.matches` | 正则匹配原始指标名；`^(.*)_total` 捕获前缀 |
| `name.as` | 重命名规则；`${1}_per_second` 把 `_total` 替换为 `_per_second` |
| `metricsQuery` | 真正发到 Prometheus 的 PromQL；`<<.Series>>` 展开为指标名，`<<.LabelMatchers>>` 展开为标签过滤条件，`<<.GroupBy>>` 展开为聚合维度（pod） |

**`metricsQuery` 展开示意：**

```
(sum(rate(my_golang_app_http_req_login_total{namespace="default", pod="login-pod-..."}[1m])) by (pod))
```

rate() 把 counter 的增量换算成每秒速率，sum() + by(pod) 按 Pod 维度聚合，得到"每个 Pod 每秒登录请求数"。

### 重新部署 prometheus-adapter 并验证

```bash
kubectl delete -f custom-metrics-apiserver-deployment.yaml
kubectl create -f custom-metrics-apiserver-deployment.yaml

# 验证指标已注册
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | python -m json.tool
```

返回中应包含：

```json
{
  "name": "namespaces/my_golang_app_http_req_login_per_second",
  "namespaced": false,
  "kind": "MetricValueList",
  "verbs": ["get"]
},
{
  "name": "pods/my_golang_app_http_req_login_per_second",
  "namespaced": true,
  "kind": "MetricValueList",
  "verbs": ["get"]
}
```

查询具体 Pod 的指标值（此时无流量，value 为 0 是正常的，因为 counter rate 无增量）：

```bash
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/%2A/my_golang_app_http_req_login_per_second" | python -m json.tool
```

```json
{
  "items": [{
    "describedObject": {
      "apiVersion": "/v1",
      "kind": "Pod",
      "name": "login-pod-deployment-65f574799c-vd55p",
      "namespace": "default"
    },
    "metricName": "my_golang_app_http_req_login_per_second",
    "selector": null,
    "timestamp": "2021-11-03T07:51:33Z",
    "value": "0"
  }]
}
```

手动压测后，value 变为 `230m`（毫/秒，即 0.23 req/s）：

```bash
# 手动访问几次触发计数
for i in {1..6}; do curl 10.96.249.88:8000/login; done

# 再次查询，value 已变为 230m
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/default/pods/%2A/my_golang_app_http_req_login_per_second"
# → "value": "230m"
```

### 编写 HPA 规则

目标：当每个 Pod 平均每秒登录请求数超过 10 时，触发扩容（最多5副本）。

```yaml
apiVersion: autoscaling/v2beta1
kind: HorizontalPodAutoscaler
metadata:
  name: loginpod-custom-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: login-pod-deployment
  minReplicas: 1
  maxReplicas: 5
  metrics:
  - type: Pods
    pods:
      metricName: my_golang_app_http_req_login_per_second
      targetAverageValue: 10    # 每个 Pod 平均 10 req/s，超过则扩容
```

```bash
kubectl apply -f loginpod-custom-hpa.yaml
kubectl get hpa loginpod-custom-hpa -w
# NAME                  REFERENCE                          TARGETS   MINPODS   MAXPODS   REPLICAS
# loginpod-custom-hpa   Deployment/login-pod-deployment    0/10      1         5         1
```

### 压测触发扩容

```bash
while true; do curl 10.96.249.88:8000/login >/dev/null; done
```

HPA 观察到指标超过阈值后，自动扩容：

```
loginpod-custom-hpa   Deployment/login-pod-deployment    73002m/10   1   5   1
loginpod-custom-hpa   Deployment/login-pod-deployment    101303m/10  1   5   4
loginpod-custom-hpa   Deployment/login-pod-deployment    76866m/10   1   5   5
```

`describe` HPA 可以看到扩容事件：

```
Type    Reason              Age    Message
Normal  SuccessfulRescale   84s    New size: 4; reason: pods metric my_golang_app_http_req_login_per_second above target
Normal  SuccessfulRescale   34s    New size: 5; reason: pods metric my_golang_app_http_req_login_per_second above target
```

同时在 Prometheus UI 查询 `sum(rate(my_golang_app_http_req_login_total[1m]))` 可以看到指标 rate 明显上升（约 67-140 req/s）。

### 完整链路总结

```
用户请求 /login
  │
  ▼ HelloHandler → myReq.Inc()        (Go counter +1)
  │
  ▼ Prometheus 抓取 /metrics           (30s interval)
  │  存储: my_golang_app_http_req_login_total
  │
  ▼ prometheus-adapter                 (PromQL: rate()[1m])
  │  注册: custom.metrics.k8s.io/v1beta1
  │  指标: pods/my_golang_app_http_req_login_per_second
  │
  ▼ HPA controller (reconcileAutoscaler)
  │  查询: custom.metrics.k8s.io
  │  计算: ceil(currentReplicas × currentQPS / targetAverageValue)
  │
  ▼ scale Deployment replicas
     login-pod-deployment: 1 → 4 → 5
```
