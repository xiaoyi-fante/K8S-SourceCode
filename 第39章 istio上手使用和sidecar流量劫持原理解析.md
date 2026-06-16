# 第39章 Istio 上手使用与 Sidecar 流量劫持原理解析

> **适用版本**: Istio 1.11.4
> **对应章节**: 第 39 章 — Istio 上手使用和 Sidecar 流量劫持原理解析
> **源码入口**: `istio/pilot/pkg/inject/webhook.go`（Sidecar 注入）、`istio/tools/istio-iptables/pkg/cmd/root.go`（iptables 规则）

---

## 核心机制一览

1. **Service Mesh 解决的问题**：微服务架构下，故障定位难、雪崩效应严重。Service Mesh 把流量治理能力（负载均衡、超时、重试、熔断、mTLS）从业务代码中剥离，下沉到 Sidecar 代理层，业务代码零改动。

2. **Istio 控制平面 = istiod**：Istio 1.11 把原先独立的 Pilot（服务发现/路由）、Citadel（证书管理）、Galley（配置校验）合并为单一 `istiod` 进程。控制平面向数据平面的每个 Envoy 推送 Discovery/Configuration/Certificates。

3. **Sidecar 注入机制**：istio 通过 apiserver 的 `MutatingWebhookConfiguration`（`istio-sidecar-injector`）拦截 Pod CREATE 请求，调用 istiod 的 `/inject` 接口，向 Pod 注入两个容器：`istio-init`（init 容器，设置 iptables 规则）和 `istio-proxy`（Envoy sidecar，转发流量）。

4. **流量劫持原理**：`istio-init` 运行 `istio-iptables` 命令，在 Pod 的网络命名空间写入 iptables 规则，把所有入站流量重定向到 Envoy 的 15006 端口，所有出站流量重定向到 15001 端口。Envoy 再根据控制平面下发的路由规则转发。

5. **四大核心 CRD**：`VirtualService`（路由规则：权重/Header 匹配/超时/故障注入）、`DestinationRule`（目标策略：subset 定义/负载均衡/熔断）、`Gateway`（Ingress/Egress 流量入口）、`ServiceEntry`（将外部服务注册到 Mesh 内）。

6. **iptables 链路径**：
   - 入站：`PREROUTING → ISTIO_INBOUND → ISTIO_IN_REDIRECT → REDIRECT 15006 → Envoy`
   - 出站：`OUTPUT → ISTIO_OUTPUT → ISTIO_REDIRECT → REDIRECT 15001 → Envoy`

---

## 全章调用链总图

```
用户请求进入 Pod
  │
  ▼ PREROUTING
  │
  ├─── ISTIO_INBOUND（拦截入站 TCP）
  │         │
  │         ▼ ISTIO_IN_REDIRECT
  │               │
  │               ▼ REDIRECT → :15006（Envoy 入站监听）
  │                     │
  │                     ▼ Envoy 根据 istiod 下发路由规则转发
  │                           │
  │                           ▼ 业务容器 :9080
  │
业务容器发出请求
  │
  ▼ OUTPUT
  │
  ├─── ISTIO_OUTPUT（检查 OwnerUID 1337）
  │         │
  │         ├─── owner UID = 1337（Envoy 自身流量）→ RETURN（不重定向，避免循环）
  │         │
  │         └─── 其他流量 → ISTIO_REDIRECT
  │                     │
  │                     ▼ REDIRECT → :15001（Envoy 出站监听）
  │                           │
  │                           ▼ Envoy 根据路由规则转发到目标 svc
```

---

## §01 微服务与 Istio 基础

### 为什么需要 Service Mesh

微服务的难点：

- 架构整体分散成多个服务，故障定位极难
- 一个服务故障可能产生雪崩效应，导致整体瘫痪

传统解法是在业务代码里集成 SDK（Hystrix、Ribbon），缺点是语言绑定、升级困难。

Service Mesh 的思路是把这些能力移到 Sidecar 代理（Envoy），业务代码不变：

- **Traffic Management（流量管理）**：协议支持 HTTP/gRPC；动态路由（权重、流量镜像、按元信息分流）；容错性（超时、重试、熔断）
- **Security（安全性）**：mTLS 加密；证书管理；强认证（SPIFFE）；授权（authentication/authorisation）
- **Observability（可观测性）**：metrics（Prometheus）；tracing（Jaeger）；流量可视化（Kiali）
- **Mesh（网格）**：多环境支持

### 控制平面与数据平面

```
                  CLI / API
                      │
         ┌────────────▼────────────┐
         │  Service Mesh Control   │  ← istiod（Pilot + Citadel + Galley）
         │       Plane             │
         └──┬──────────┬───────────┘
            │          │  Discovery / Configuration / Certificates
    ┌───────▼──┐  ┌────▼─────┐  ┌───────────┐
    │Instance A│  │Instance B│  │Instance C │   ← 业务 Pod
    │          │  │          │  │           │
    │ Sidecar  │  │ Sidecar  │  │ Sidecar   │   ← Envoy 代理（数据平面）
    └──────────┘  └──────────┘  └───────────┘
         East-West Traffic（服务间流量）
```

- **数据平面**：管理实例之间的网络流量（东西向）
- **控制平面**：负责生成和部署控制数据平面行为的相关配置（南北向）

### Istio 架构（1.11）

```
                     Ingress traffic
                          │
 ┌───────────────────────────────────────────────────┐
 │  Istio Mesh                                       │
 │                                                   │
 │   Service A          ←Mesh traffic→  Service B    │
 │      │                                    │       │
 │   Proxy(Envoy)                        Proxy(Envoy)│
 │                                                   │
 │         Discovery/Configuration/Certificates      │
 │    ┌──────────────────────────────────────────┐   │
 │    │  istiod  [Pilot] [Citadel] [Galley]      │   │
 │    └──────────────────────────────────────────┘   │
 └───────────────────────────────────────────────────┘
                                           │
                                     Egress traffic
```

Istio 可以不改业务代码，通过 Sidecar 代理实现：负载均衡、mTLS、监控、流量控制等。

---

## §02 Istio 安装部署

### 安装步骤

```bash
# 下载 istioctl 和对应 yaml
wget https://github.com/istio/istio/releases/download/1.11.4/istio-1.11.4-linux-amd64.tar.gz
tar xf istio-1.11.4-linux-amd64.tar.gz
/bin/cp -f bin/istioctl /usr/bin/

# 使用 demo profile 安装（含 Prometheus/Grafana/Jaeger/Kiali）
istioctl install --set profile=demo -y
# 输出：Istio core installed / Istiod installed / Egress gateways installed / Ingress gateways installed

# 给命名空间打标签，启用自动 Sidecar 注入
kubectl label namespace default istio-injection=enabled
```

### 部署示例应用 bookinfo

```bash
kubectl apply -f samples/bookinfo/platform/kube/bookinfo.yaml
```

bookinfo 包含四个微服务：`details`、`productpage`、`ratings`、`reviews`（v1/v2/v3 三版本）。

对外暴露 productpage：

```bash
# 查 istio-ingressgateway 的 NodePort
export INGRESS_PORT=$(kubectl -n istio-system get service istio-ingressgateway -o jsonpath='{.spec.ports[?(@.name=="http2")].nodePort}')
# 访问：nodeip:$INGRESS_PORT/productpage
```

刷新多次页面，Book Reviews 星星评分会在三种情况随机出现：空（v1）、5 颗红色（v2）、5 颗黑色（v3），说明流量默认轮流打到三个 reviews 版本。

### 可观测性工具栈

istioctl install demo profile 同时部署了：

| 工具 | 功能 | 暴露方式 |
|------|------|---------|
| Kiali | 服务拓扑可视化（graph）、流量路径 | NodePort:10002 |
| Prometheus | 指标采集（apiserver/cadvisor/pod 自定义指标） | NodePort:10003 |
| Grafana | 指标展示（Istio Control Plane/Mesh/Service/Workload Dashboard） | NodePort:10004 |
| Jaeger | 分布式链路追踪 | NodePort（tracing） |

所有组件都以 Deployment 部署在 `istio-system` namespace。

#### Prometheus 采集源分析

Prometheus 通过 config（configmap）配置三类采集 job：

**① apiserver 指标** (`job_name: kubernetes-apiservers`)：通过 k8s endpoint SD 发现 `kubernetes` svc 的 endpoint，即直接 scrape apiserver。

**② 容器基础资源指标** (`job_name: kubernetes-nodes-cadvisor`)：通过 k8s node SD 发现所有 node，构造 path：`/api/v1/nodes/<node_name>/proxy/metrics/cadvisor`，经 apiserver proxy 到 kubelet 内置的 cadvisor。

**③ Pod 自定义指标** (`job_name: kubernetes-pods`)：通过 k8s pod SD 发现 pod，只采集 annotations 中含 `prometheus.io/scrape: "true"` 的 pod（`action: keep` 规则过滤），端口和 path 从 `prometheus.io/port` 和 `prometheus.io/path` annotation 读取。Pod 如需暴露自定义指标，在 `spec.template.metadata.annotations` 中配置：

```yaml
annotations:
  prometheus.io/scrape: 'true'
  prometheus.io/port: '8080'
  prometheus.io/path: 'metrics'
```

#### Grafana Dashboard

Grafana 数据源配置在 configmap 中，datasource URL 为 `http://prometheus:9090`（同 namespace DNS 解析）。提供 Istio 专用 Dashboard：`Istio Control Plane Dashboard`、`Istio Mesh Dashboard`、`Istio Service Dashboard`、`Istio Workload Dashboard` 等。

---

## §03 基于身份的请求路由、故障注入、流量转移

### 01 默认路由规则

先应用目标规则（定义 subset）：

```bash
kubectl apply -f samples/bookinfo/networking/destination-rule-all-mtls.yaml
kubectl get destinationrules -o yaml
```

### 02 VirtualService 路由

应用 VirtualService，把所有流量路由到 reviews v1（无星星）：

```bash
kubectl apply -f samples/bookinfo/networking/virtual-service-all-v1.yaml
```

对应 YAML：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: reviews
spec:
  hosts:
    - reviews
  http:
    - route:
      - destination:
          host: reviews
          subset: v1
```

### 基于用户身份的路由

将来自 HTTP Header `end-user: jason` 的请求路由到 reviews:v2，其余路由到 v1：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: reviews
spec:
  hosts:
    - reviews
  http:
    - match:
        - headers:
            end-user:
              exact: jason
      route:
        - destination:
            host: reviews
            subset: v2
    - route:
        - destination:
            host: reviews
            subset: v1
```

Istio 对用户身份没有内置机制，productpage 在调用 reviews 时在 HTTP 请求 Header 中附加了自定义的 `end-user` 字段。用 jason 身份登录后，刷新页面可以看到星级评分（reviews:v2）；其他用户仍然看到 v1（无星）。

### 03 故障注入

#### 注入 HTTP 延迟故障

为用户 jason 在 reviews:v2 → ratings 之间注入 7 秒延迟：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: ratings
spec:
  hosts:
    - ratings
  http:
    - match:
        - headers:
            end-user:
              exact: jason
      fault:
        delay:
          percentage:
            value: 100.0
          fixedDelay: 7s
      route:
        - destination:
            host: ratings
            subset: v1
    - route:
        - destination:
            host: ratings
            subset: v1
```

结果：页面加载约 6 秒后显示错误（Reviews 部分）。原理：reviews 服务对 ratings 的超时硬编码为 10 秒，productpage 对 reviews 的超时硬编码为 3 秒，7 秒延迟超过了 3 秒限制，导致 reviews 调用超时报错给 productpage。这揭示了一个微服务中硬编码超时的 bug。

#### 注入 HTTP abort 故障

```yaml
fault:
  abort:
    percentage:
      value: 100.0
    httpStatus: 500
```

应用后，jason 登录访问 productpage，页面显示 "Ratings service is currently unavailable"。Kiali graph 中可以看到红色 500 trace。

### 04 流量转移（金丝雀发布）

将 reviews 流量逐步从 v1 迁移到 v3：

```bash
# 第一步：50% 流量到 v3
kubectl apply -f samples/bookinfo/networking/virtual-service-reviews-50-v3.yaml
```

```yaml
spec:
  hosts:
    - reviews
  http:
    - route:
        - destination:
            host: reviews
            subset: v1
          weight: 50
        - destination:
            host: reviews
            subset: v3
          weight: 50
```

约 50% 的刷新可以看到红色星星（v3）。确认 v3 稳定后，切换 100% 流量到 v3：

```bash
kubectl apply -f samples/bookinfo/networking/virtual-service-reviews-v3.yaml
```

通过调整多个 subset 的 `weight`，可以模拟金丝雀发布，实现流量的平滑迁移。

---

## §04 访问外部服务

### 01 Envoy 默认的出站流量策略

Istio 默认 `global.outboundTrafficPolicy.mode = ALLOW_ANY`，Sidecar 可以放行到外部的任何 HTTP 请求。验证：

```bash
export SOURCE_POD=$(kubectl get pod -l app=sleep -o jsonpath={.items..metadata.name})
kubectl exec "$SOURCE_POD" -c sleep -- curl -sSI https://baidu.com | grep "HTTP/"
# HTTP/1.1 302  ← 说明可以访问外部
```

这种访问方式的缺点：丢失了对外部服务调用的 Istio 监控和流量控制能力（不记录 Mixer 日志）。

### 03 更改为默认封锁策略

将 `global.outboundTrafficPolicy.mode` 改为 `REGISTRY_ONLY`：

```bash
istioctl install --set profile=demo --set meshConfig.outboundTrafficPolicy.mode=REGISTRY_ONLY
```

改为 REGISTRY_ONLY 后，Pod 访问未注册的外部服务会被拒绝（SSL/连接错误，exit code 35）。

### 04 ServiceEntry — 允许访问外部 HTTP 服务

将外部服务注册到 Mesh 的服务注册表，允许流量通过并受 Istio 策略管控：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: ServiceEntry
metadata:
  name: httpbin-ext
spec:
  hosts:
    - httpbin.org
  ports:
    - number: 80
      name: http
      protocol: HTTP
  resolution: DNS
  location: MESH_EXTERNAL
```

创建后访问 httpbin.org，响应 Header 中可以看到 Istio Sidecar 添加的 `X-Envoy-Decorator-Operation` 等字段，证明流量经过了 Envoy 代理。同样可以创建 HTTPS 的 ServiceEntry：

```yaml
kind: ServiceEntry
metadata:
  name: baidutls
spec:
  hosts:
    - www.baidu.com
  ports:
    - number: 443
      name: https
      protocol: HTTPS
  resolution: DNS
  location: MESH_EXTERNAL
```

### 05 使用 VirtualService 管理外部服务流量

结合 ServiceEntry + VirtualService，可以对外部服务设置路由规则（如超时）：

```yaml
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: httpbin-ext
spec:
  hosts:
    - httpbin.org
  http:
    - timeout: 3s
      route:
        - destination:
            host: httpbin.org
          weight: 100
```

测试：访问 httpbin.org/delay 接口（响应需 5 秒），Istio 在 3 秒后返回 504，证明超时规则生效。

### 06 让特定 IP 范围绕过 Istio

```bash
istioctl install --set profile=demo --set values.global.proxy.includeIPRanges="10.0.0.1/24"
```

只有匹配该 CIDR 的流量才进入 Istio 代理，其余流量直接放行。

**本节重点总结**：Istio 访问外部服务的三种方式：
1. `ALLOW_ANY`（默认）：Envoy 允许所有出站流量，但丢失监控和流量控制能力
2. 创建 `ServiceEntry`：将外部服务注册到 Mesh，Istio 监控和流量控制完整生效
3. 配置 `includeIPRanges`：让特定 IP 范围完全绕过 Istio 代理

---

## §05 Sidecar 注入讲解

**本节重点**：istio 中的 Sidecar 通过 apiserver 的 `MutatingWebhookConfiguration` 注入了两个容器：
- `istio-init`：init 容器，负责初始化 iptables 规则
- `istio-proxy`（底层是 Envoy）：负责流量转发

### 01 Sidecar 注入示例分析

以 bookinfo productpage 为例，原始 Deployment YAML 只声明了一个业务容器：

```yaml
containers:
  - name: productpage
    image: docker.io/istio/examples-bookinfo-productpage-v1:1.16.2
    ports:
      - containerPort: 9080
```

执行 `kubectl get pod -l app=productpage -o yaml` 后，实际 Pod 多了：

**istio-init（init容器）**：

```yaml
initContainers:
  - args:
      - istio-iptables
      - -p
      - "15001"       # ENVOY_PORT（出站）
      - -z
      - "15006"       # InboundCapturePort（入站）
      - -u
      - "1337"        # ProxyUID（Envoy 进程 UID）
      - -m
      - REDIRECT      # InboundInterceptionMode
      - -i
      - '*'           # 重定向所有出站流量
      - -x
      - ""            # 排除的出站 CIDR（空=无排除）
      - -b
      - '*'           # 重定向所有入站端口
      - -d
      - 15090,15021,15020  # 排除的入站端口（Envoy 自身管理端口）
    image: docker.io/istio/proxyv2:1.11.4
    name: istio-init
    securityContext:
      capabilities:
        add:
          - NET_ADMIN  # 需要此权限操作 iptables
          - NET_RAW
```

**istio-proxy（sidecar容器）**：

```yaml
containers:
  - name: istio-proxy
    image: docker.io/istio/proxyv2:1.11.4
    ports:
      - containerPort: 15090   # http-envoy-prom（Envoy metrics）
    readinessProbe:
      httpGet:
        path: /healthz/ready
        port: 15021
    securityContext:
      runAsGroup: 1337
      runAsUser: 1337          # 关键：UID 1337，iptables OUTPUT 规则会 RETURN 此 UID 的流量，避免循环
```

### 02 istio 准入控制器源码讲解

注入逻辑入口（istio 源码，非 k8s 本地库）：

```
D:\go_path\src\github.com\istio\istio\pkg\kube\inject\webhook.go
```

注册了 `/inject` 这个 Handler，主要函数是 `wh.serveInject`，调用 `wh.inject(ar, path)` 执行注入。

`MutatingWebhookConfiguration` 配置：

```yaml
name: istio-sidecar-injector
webhooks:
  - name: object.sidecar-injector.istio.io
    clientConfig:
      service:
        name: istiod
        namespace: istio-system
        path: /inject
        port: 443
    namespaceSelector:
      matchExpressions:
        - key: istio-injection
          operator: DoesNotExist     # 不在有 istio-injection 标签的 ns 触发
    objectSelector:
      matchExpressions:
        - key: sidecar.istio.io/inject
          operator: In
          values:
            - "true"                 # Pod 需要有此标签才触发
    rules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["CREATE"]
        resources: ["pods"]
```

注入内容（init 容器 args）通过模板动态生成，核心参数由 annotation 控制，如 `sidecar.istio.io/interceptionMode`（`-m` 参数）、`traffic.sidecar.istio.io/includeOutboundIPRanges`（`-i` 参数）等。

---

## §06 Sidecar 流量劫持解析

| 读码目标 | 工具 | 入口命令 |
|---------|------|---------|
| istio-iptables 参数定义 | istio 源码 | `istio/tools/istio-iptables/pkg/cmd/root.go` |
| iptables 链构建 | istio 源码 | `capture/run.go` → `AppendRule` |
| 验证实际规则 | nsenter | `nsenter -t <pid> -n iptables -t nat -nvL` |

### 01 init 容器启动参数解析

完整命令：

```
istio-iptables -p "15001" -z "15006" -u "1337" -m REDIRECT -i '*' -x "" -b '*' -d 15090,15021,15020
```

参数含义：

| 参数 | 含义 | 值 |
|------|------|----|
| `-p` | `ENVOY_PORT`：出站流量重定向端口 | 15001 |
| `-z` | `InboundCapturePort`：入站流量重定向端口 | 15006 |
| `-u` | `ProxyUID`：不重定向该 UID 的流量（Envoy 自身） | 1337 |
| `-m` | `InboundInterceptionMode`：REDIRECT 或 TPROXY | REDIRECT |
| `-i` | 重定向出站流量的 CIDR 范围，`*` = 所有 | `*` |
| `-x` | 排除出站重定向的 CIDR 范围 | 空（无排除） |
| `-b` | 重定向入站流量的端口列表，`*` = 所有 | `*` |
| `-d` | 排除入站重定向的端口（Envoy 自身管理端口） | 15090,15021,15020 |

### 源码实现

init 容器 ENTRYPOINT 是 `pilot-agent`（Dockerfile 位置：`istio/pilot/docker/Dockerfile.proxyv2`）。`pilot-agent` 注册子命令 `istio-iptables`，入口在 `istio/tools/istio-iptables/pkg/cmd/root.go`：

```go
var rootCmd = &cobra.Command{
    Use:   "istio-iptables",
    Short: "Set up iptables rules for Istio Sidecar",
    Long:  "istio-iptables is responsible for setting up port forwarding for Istio Sidecar.",
    Run: func(cmd *cobra.Command, args []string) {
        cfg := constructConfig()
        var ext dep.Dependencies
        if cfg.DryRun {
            ext = &dep.StdoutStubDependencies{}
        } else {
            ext = &dep.RealDependencies{
                CNIMode:          cfg.CNIMode,
                NetworkNamespace: cfg.NetworkNamespace,
            }
        }
    },
}
```

核心是 `capture/run.go` 中的 `IptablesConfigurator`，通过 `AppendRule` 函数构建 iptables 链：

```go
func (cfg *IptablesConfigurator) Run() {
    // 创建新链
    cfg.iptables.AppendRule(iptables.NatTable, constants.ISTIO_INBOUND, ...)
    cfg.iptables.AppendRule(iptables.NatTable, constants.ISTIO_IN_REDIRECT, ...)
    cfg.iptables.AppendRule(iptables.NatTable, constants.ISTIO_OUTPUT, ...)
    cfg.iptables.AppendRule(iptables.NatTable, constants.ISTIO_REDIRECT, ...)
}
```

### 最终 iptables 规则验证

用 `nsenter` 进入 Pod 网络命名空间查看规则：

```bash
# 找到 productpage 容器的 PID
ps -ef | grep productpage
# 进入其网络命名空间，查看 NAT 表
nsenter -t <pid> -n iptables -t nat -nvL
```

#### 入站流量链路（对于进入 productpage 的请求）

```
PREROUTING：
  47818 2869K ISTIO_INBOUND  tcp  --  *  *  0.0.0.0/0  0.0.0.0/0
                        ↓
ISTIO_INBOUND（拦截入站 TCP）：
  - RETURN  tcp  dpt:15008   ← 排除已是 Envoy 的流量
  - RETURN  tcp  dpt:22
  - RETURN  tcp  dpt:15090
  - RETURN  tcp  dpt:15021
  - ISTIO_IN_REDIRECT  tcp  --  *  *  ← 其余全部重定向
                        ↓
ISTIO_IN_REDIRECT：
  47608 2869K REDIRECT  tcp  --  *  *  redir ports 15006
```

所有入站 TCP 流量最终被重定向到 `:15006`（Envoy 入站监听端口）。

#### 出站流量链路（productpage 发出的请求）

```
OUTPUT：
  47760 2880K ISTIO_OUTPUT  tcp  --  *  *  0.0.0.0/0  0.0.0.0/0
                        ↓
ISTIO_OUTPUT：
  - RETURN  all  lo  0.0.0.7  ← lo 接口、owner UID 1337 的流量（Envoy 自身）RETURN，避免循环
  - RETURN  all  lo  !127.0.1  owner UID 匹配
  - RETURN  all  *  owner GID 匹配
  - ISTIO_REDIRECT  all  --  *  *  0.0.0.0/0  ← 其余全部送入 ISTIO_REDIRECT
                        ↓
ISTIO_REDIRECT：
  643K 39M REDIRECT  tcp  --  *  *  redir ports 15001
```

所有出站流量（除 Envoy 自身 UID=1337 的流量）被重定向到 `:15001`（Envoy 出站监听端口）。

**UID=1337 的豁免**是关键设计：Envoy（istio-proxy）以 UID 1337 运行，其自身发出的流量不能再次被重定向，否则会产生无限循环。ISTIO_OUTPUT 链通过 `owner UID match 1337 → RETURN` 打破了这个循环。

### 本节重点总结

istio 劫持入站流量的路径：
```
REQUEST → (IPTABLES PREROUTING → ISTIO_INBOUND → ISTIO_IN_REDIRECT → REDIRECT PORT 15006) → Envoy 入站
```

istio 劫持出站流量的路径：
```
REQUEST → (IPTABLES OUTPUT → ISTIO_OUTPUT → ISTIO_REDIRECT → REDIRECT PORT 15001) → Envoy 出站
```
