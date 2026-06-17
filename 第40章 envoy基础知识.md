# 第40章 Envoy 基础知识

> **适用版本**: Envoy（独立项目，非 Kubernetes 源码）
> **对应章节**: 第 40 章 — Envoy 基础知识
> **源码入口**: 无本地源码，内容来自课程截图

---

## 核心机制一览

1. **四大配置类型**：Envoy 的所有流量控制通过 Listeners（监听器）、Filters（过滤器）、Routers（路由）、Clusters（集群）四类对象组合表达，等价于 Nginx 的 server/location/upstream 三段式，但分层更清晰。

2. **静态 vs 动态配置**：静态配置（`static_resources`）写死在 YAML 中，启动时加载；动态配置（`dynamic_resources`）通过 EDS/CDS/LDS 等 xDS API 实时推送，Envoy 无需重启即可更新集群和监听器。

3. **xDS 体系**：EDS（端点发现）、CDS（集群发现）、RDS（路由发现）、LDS（监听器发现）、VHDS（虚拟主机发现）、SDS（密钥发现）六类 xDS API，共同组成 Envoy 的动态控制平面协议，Istio 的 istiod 正是通过 xDS 下发配置给 Envoy。

4. **TLS 终止**：Envoy 通过 `tls_context` 在监听器上挂载证书，在 Cluster 上配置 `tls_context.sni` 实现 HTTPS 代理；`https_redirect: true` 实现 HTTP→HTTPS 301 跳转，无需业务代码参与。

5. **文件热更新触发方式**：基于文件的动态配置（EDS/CDS/LDS）依赖 inotify rename 事件，直接覆盖写不触发，必须用 `mv old tmp && mv tmp old` 的原子替换方式。

6. **EDS server 断连容忍**：Envoy 一旦通过 REST/gRPC API 完成端点发现，即使 EDS server 断连，已缓存的集群节点依然可用——控制平面故障不影响数据平面转发。

---

## §01 Envoy 基础知识与静态配置

### 为什么学 Envoy

Istio 使用 Envoy 作为数据平面 Sidecar，理解 Envoy 的配置模型是理解 Istio 流量管理的前提。

官方中文文档：https://cloudnative.to/envoy/intro/what_is_envoy.html

### Envoy 简介与核心能力

Envoy 是专为大型现代 SOA 架构设计的 L7 代理和通信总线：

- 进程外运行：以 Sidecar 方式透明代理，业务应用无感知，不依赖特定语言
- 基于现代 C++11 实现，性能接近 L3/L4 直接转发
- L3/L4 过滤器架构，支持 TCP/UDP 代理，TLS 认证
- HTTP L7 过滤器：独立的 HTTP 连接管理器，支持 HTTP/1.1 和 HTTP/2
- gRPC 支持：HTTP/2 之上的透明代理
- 可观测性：内置 stats 端点，兼容 Prometheus；支持 distributed tracing（zipkin/jaeger）

### Envoy 静态配置的四大概念

Envoy 使用 YAML 配置，分静态（`static_resources`）和动态（`dynamic_resources`）两种。静态配置由四类对象组成：

```
请求进入
    │
    ▼
┌─────────────────────────────────┐
│ 01 资源 (Resources)              │
│   Envoy 唯一行为：从监听器接收请求  │
└───────────────┬─────────────────┘
                │
    ▼
┌─────────────────────────────────┐
│ 02 监听器 (Listeners)            │
│   定义监听地址和端口               │
│   收到请求后交给过滤器链处理         │
└───────────────┬─────────────────┘
                │
    ▼
┌─────────────────────────────────┐
│ 03 过滤器 (Filters)              │
│   每个过滤器都接收请求并传给下一个    │
│   envoy.http_connection_manager  │
│   处理 HTTP；还有 Redis/Mongo/TCP │
└───────────────┬─────────────────┘
                │ 路由匹配
    ▼
┌─────────────────────────────────┐
│ 04 集群 (Clusters)               │
│   请求匹配后转发到集群              │
│   等同于 nginx upstream           │
│   支持 ROUND_ROBIN/LEAST_REQUEST  │
└─────────────────────────────────┘
```

### 完整静态配置示例

以代理 baidu.com 为例，监听 10000 端口，将请求转发到 baidu.com HTTPS：

```yaml
static_resources:
  listeners:
  - name: listener_0
    address:
      socket_address: { address: 0.0.0.0, port_value: 10000 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { host_rewrite: www.baidu.com, cluster: service_baidu }
          http_filters:
          - name: envoy.router

  clusters:
  - name: service_baidu
    connect_timeout: 0.25s
    type: LOGICAL_DNS
    dns_lookup_family: V4_ONLY
    lb_policy: ROUND_ROBIN
    hosts: [{ socket_address: { address: www.baidu.com, port_value: 443 }}]
    tls_context: { sni: baidu.com }

admin:
  access_log_path: /tmp/admin_access.log
  address:
    socket_address: { address: 0.0.0.0, port_value: 10001 }
```

**关键点**：
- `LOGICAL_DNS`：每次建连时解析 DNS，适合域名可能变化的外部服务
- `tls_context.sni`：指定 TLS SNI，Envoy 以 HTTPS 访问上游
- Admin 后台监听 10001，访问 `localhost:10001` 可查看 stats、集群、路由等运行状态

启动命令：

```bash
docker run --name=envoy-with-admin -d \
  -p 15000:10000 \
  -p 15001:9901 \
  -v $(pwd)/envoy.yaml:/etc/envoy/envoy.yaml \
  envoyproxy/envoy:latest
```

### 迁移 Nginx 到 Envoy

Nginx 有 `worker_processes`（进程模型）、`upstream`（负载均衡）、`server`/`location`（路由）三段配置。对应到 Envoy：

| Nginx 概念 | Envoy 等价 | 说明 |
|-----------|-----------|------|
| `worker_processes` | 无需配置 | Envoy 单进程多线程，master 管理，worker 负责监听/过滤/转发 |
| `upstream` | `clusters` | 定义后端目标，Envoy 可配超时和更细粒度负载均衡 |
| `server { listen; server_name }` | `listeners` | 定义监听地址端口 |
| `location { proxy_pass }` | `filters + route_config` | 过滤器+路由配置 |
| `gzip on` / `access_log` | `http_filters` | Envoy 内置过滤器处理 |

Nginx 使用 `STRICT_DNS`（启动时一次性解析），Envoy 的 `LOGICAL_DNS` 每次建连时重新解析，更适合动态 IP 的外部服务。

### 日志配置

Envoy 采用云原生方式，应用日志默认输出到 stdout/stderr：

```yaml
- name: envoy.http_connection_manager
  config:
    access_log:
    - name: envoy.file_access_log
      config:
        path: "/dev/stdout"
        format: "[%START_TIME%] \"%REQ(:METHOD)% %REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL%\" %RESPONSE_CODE% ..."
```

`format` 字段类比 nginx 的 `log_format`，可自定义字段顺序和内容。

### 验证：两后端负载均衡

启动两个 http-server 容器（端口 81/82），Envoy 代理后轮询分发：

```bash
docker run -p 81:80 -d cnych/docker-http-server
docker run -p 82:80 -d cnych/docker-http-server
# 连续 curl 可见响应交替来自两个不同的 container id
```

---

## §02 Envoy 代理 HTTPS 流量

### 制作 SSL 证书

```bash
mkdir -p certs
openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout certs/example-com.key \
  -out certs/example-com.crt \
  -days 365 \
  -subj '/CN=*.example.com'
```

### Envoy TLS 配置

在 HTTPS 监听器上挂载证书，通过 `tls_context` 指定：

```yaml
- name: listener_https
  address:
    socket_address: { address: 0.0.0.0, port_value: 8443 }
  filter_chains:
  - filters:
    - name: envoy.http_connection_manager
      config:
        ...
        route_config:
          virtual_hosts:
          - name: backend
            domains: ["example.com"]
            routes:
            - match: { prefix: "/service/1" }
              route: { cluster: service1 }
            - match: { prefix: "/service/2" }
              route: { cluster: service2 }
        http_filters:
        - name: envoy.router
    tls_context:
      common_tls_context:
        tls_certificates:
        - certificate_chain:
            filename: "/etc/envoy/certs/example-com.crt"
          private_key:
            filename: "/etc/envoy/certs/example-com.key"
```

### HTTP 自动跳转 HTTPS

在 HTTP 监听器（8080）的路由中加 `https_redirect: true`：

```yaml
route_config:
  virtual_hosts:
  - name: backend
    domains: ["example.com"]
    routes:
    - match:
        prefix: "/"
      redirect:
        path_redirect: "/"
        https_redirect: true
```

匹配到后 Envoy 返回 301，浏览器自动跳转到 HTTPS 端口。

### 完整双监听器架构

```
客户端 HTTP  :8080
    │
    ▼ https_redirect: true
    301 → https://example.com
    │
客户端 HTTPS :8443
    │
    ▼ tls_context（证书终止）
    ▼ filter: envoy.http_connection_manager
    │
    ├─ /service/1 → cluster: service1 (port 81)
    └─ /service/2 → cluster: service2 (port 82)
```

### 测试

```bash
docker run --rm -it --name proxy1 \
  -p 80:8080 -p 443:8443 -p 8001:8001 \
  -v $(pwd):/etc/envoy envoyproxy/envoy:latest

# HTTP 请求 → 自动 301
curl -H "Host: example.com" http://localhost -i
# HTTP/1.1 301 Moved Permanently

# HTTPS 请求（-k 忽略自签证书）
curl -k -H "Host: example.com" https://localhost/service/1 -i
# HTTP/1.1 200 OK，header: x-envoy-upstream-service-time: 1
```

Admin 页面可查看证书有效期：`curl http://localhost:8001/certs`

**本节要点**：`tls_context` 在 Listener 的 filter_chain 上做 TLS 终止；`https_redirect: true` 实现无代码的 HTTP→HTTPS 跳转。

---

## §03 基于文件的动态 EDS 和 CDS 配置

### Envoy 动态配置 API 体系

| xDS API | 全名 | 发现内容 |
|---------|------|---------|
| EDS | Endpoint Discovery Service | 集群的端点（IP:Port） |
| CDS | Cluster Discovery Service | 集群定义 |
| RDS | Route Discovery Service | 路由规则 |
| LDS | Listener Discovery Service | 监听器 |
| VHDS | Virtual Host Discovery Service | 虚拟主机（HTTP 路由子集） |
| SDS | Secret Discovery Service | TLS 证书和私钥 |

动态配置有两种来源：**文件**（inotify 监听）或 **REST-JSON/gRPC 端点**。

### 动态 EDS：集群端点热更新

`static_resources` 中集群改为 `type: EDS`，并指定 eds.yaml 文件路径：

```yaml
# envoy.yaml 中的 cluster
clusters:
- name: targetCluster
  connect_timeout: 0.25s
  lb_policy: ROUND_ROBIN
  type: EDS
  eds_cluster_config:
    service_name: localservices
    eds_config:
      path: '/etc/envoy/eds.yaml'
```

对应的 `eds.yaml`（ClusterLoadAssignment 格式）：

```yaml
version_info: "0"
resources:
- "@type": "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment"
  cluster_name: "localservices"
  endpoints:
  - lb_endpoints:
    - endpoint:
        address:
          socket_address:
            address: "172.20.70.205"
            port_value: 81
```

**热更新触发**：Docker Volume 挂载场景下，直接写文件有时不触发 inotify，需用原子替换：

```bash
mv eds.yaml tmp && mv tmp eds.yaml
```

更新后 Envoy 自动加载新端点，无需重启。

### 动态 CDS + LDS：集群和监听器热更新

`envoy.yaml` 改为 `dynamic_resources` 模式，同时指定 CDS 和 LDS 文件：

```yaml
node:
  id: id_1
  cluster: test

dynamic_resources:
  lds_config:
    path: "/etc/envoy/lds.yaml"
  cds_config:
    path: "/etc/envoy/cds.yaml"
```

`cds.yaml` 定义集群列表（包含 EDS 引用）：

```yaml
version_info: "0"
resources:
- "@type": "type.googleapis.com/envoy.api.v2.Cluster"
  name: targetCluster
  connect_timeout: 0.25s
  lb_policy: ROUND_ROBIN
  type: EDS
  eds_cluster_config:
    service_name: localservices
    eds_config:
      path: /etc/envoy/eds.yaml
- "@type": "type.googleapis.com/envoy.api.v2.Cluster"
  name: newTargetCluster
  connect_timeout: 0.25s
  lb_policy: ROUND_ROBIN
  type: EDS
  eds_cluster_config:
    service_name: localservices
    eds_config:
      path: /etc/envoy/eds1.yaml
```

`lds.yaml` 定义监听器（路由中 `/old` → targetCluster，`/new` → newTargetCluster）：

```yaml
version_info: "0"
resources:
- "@type": "type.googleapis.com/envoy.api.v2.Listener"
  name: listener_0
  address:
    socket_address: { address: 0.0.0.0, port_value: 10000 }
  filter_chains:
  - filters:
    - name: envoy.http_connection_manager
      config:
        ...
        routes:
        - match: { prefix: "/old" }
          route: { cluster: targetCluster }
        - match: { prefix: "/new" }
          route: { cluster: newTargetCluster }
```

更新 cds.yaml 或 lds.yaml 后原子替换，Envoy 自动感知并应用新配置。

**本节要点**：EDS/CDS/LDS 文件热更新必须用 rename 原子替换；`dynamic_resources` 使 envoy.yaml 本身极简，所有动态内容由外部文件驱动。

---

## §04 基于 API 的动态端点发现

### 架构

```
Envoy
  │
  │ REST API (refresh_delay: 5s)
  ▼
EDS Server (Flask, port 7779)
  │
  │ POST/PUT /edsservice/<service_name>
  ▼
hosts: [{ip, port, tags}]
```

EDS server 是一个 REST-JSON 服务（课程使用 `cnych/eds_server` 镜像，基于 Flask），提供端点注册/更新/删除 API。

### Envoy 配置

`envoy.yaml` 中 cluster 使用 `api_type: REST`，并定义 eds_cluster（静态 STATIC 类型）指向 EDS server：

```yaml
node:
  cluster: mycluster
  id: test-id

static_resources:
  listeners:
  - name: listener_0
    address:
      socket_address: { address: 0.0.0.0, port_value: 10000 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          ...
          routes:
          - match: { prefix: "/" }
            route: { cluster: targetCluster }

  clusters:
  - name: targetCluster
    type: EDS
    connect_timeout: 0.25s
    eds_cluster_config:
      service_name: myservice
      eds_config:
        api_config_source:
          api_type: REST
          cluster_names: [eds_cluster]
          refresh_delay: 5s
  # eds_cluster 本身用 STATIC 类型，指向 EDS server
  - name: eds_cluster
    type: STATIC
    connect_timeout: 0.25s
    hosts: [{ socket_address: { address: 172.20.70.205, port_value: 7779 }}]
```

### 启动 EDS Server

```bash
docker run --name=eds_server -p 7779:8080 -d cnych/eds_server
# 服务启动后日志显示：
# POST /v2/discovery:endpoints HTTP/1.1 200
```

### 注册和更新端点

注册单个端点（POST）：

```bash
curl -X POST \
  --header 'Content-Type: application/json' \
  --header 'Accept: application/json' \
  -d '{
    "hosts": [{
      "ip_address": "172.20.70.205",
      "port": 81,
      "tags": {
        "az": "cn-beijing-a",
        "canary": false,
        "load_balancing_weight": 50
      }
    }]
  }' \
  http://localhost:7779/edsservice/myservice
```

批量注册 4 个端点（PUT，端口 81/82/83/84），Envoy 5s 内自动感知，流量均摊到 4 个后端。

### 清空端点与 EDS 断连容忍

```bash
# 清空所有端点 → Envoy 返回 503 no healthy upstream
curl -X PUT \
  --header 'Content-Type: application/json' \
  -d '{"hosts": []}' \
  http://localhost:7779/edsservice/myservice
```

**关键验证**：恢复端点注册后，停掉 EDS server，Envoy 已发现的节点依然正常转发。EDS server 只是控制平面，断连不影响数据平面已缓存的集群状态。

```bash
# EDS server 停掉后，curl 依然 200
while true; do curl localhost; sleep 2; done
# <h1>This request was processed by host: ...</h1>  (持续正常)
```

**本节要点**：
- EDS server 通过 REST API 提供端点发现，Envoy 按 `refresh_delay` 轮询
- 即使 EDS server 断连，已发现的集群节点不受影响——控制平面故障不影响数据平面
