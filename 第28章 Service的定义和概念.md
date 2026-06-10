# 第28章 Service 的定义和概念

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 28 章 — Service 的定义和概念
> **源码入口**: 概念与演示章节，无核心源码分析

---

## 核心机制一览

1. **Service 是 Pod 的稳定代理**：Pod IP 随时变化，Service 提供一个固定的 ClusterIP + DNS 域名作为访问入口，底层由 kube-proxy 维护 iptables/ipvs 规则将流量转发到后端 Pod。

2. **四种暴露类型，覆盖从集群内到集群外的全部场景**：ClusterIP（集群内）→ NodePort（节点端口对外）→ LoadBalancer（云厂商 LB 对外）→ ExternalName（CNAME 映射外部服务）。Ingress 是独立的七层路由层，不是 Service 的一种类型。

3. **四种代理模式，性能从低到高**：userspace（流量过一次内核+用户态，已淘汰）→ iptables（DNAT，当前默认，基于 netfilter，不会自动重试）→ ipvs（哈希表 + 多种负载均衡算法，大规模集群首选）→ kernelspace（Windows 专用）。

4. **两种服务发现，推荐 DNS**：环境变量（kubelet 注入，要求 Pod 创建前 Service 已存在）与 CoreDNS（`{svc}.{namespace}.svc.cluster.local`，无顺序限制）。Pod 内 `/etc/resolv.conf` 配置三个搜索域，短名访问会依次补全。

5. **Headless Service 专为 StatefulSet 设计**：`clusterIP: None`，不分配 ClusterIP，DNS 查询直接返回后端 Pod IP 的 A 记录而非 VIP，使 StatefulSet 中的 Pod 各自拥有稳定的 DNS 域名（`{pod-name}.{svc}.{namespace}.svc.cluster.local`），供分布式系统（ES、etcd）在配置文件中直接写域名互相发现。

---

## §01 四种 Service 类型

### Service 是什么，为什么需要它

在 Kubernetes 中，Pod 的 IP 地址随时可能变化（重建、漂移），且 Deployment 会同时维护多个副本 Pod。Service 是一组同 label 类型 Pod 的服务抽象，为逻辑上的一组 Pod 提供了一致的访问策略，同时提供 k8s 中的负载均衡机制。

```
Traffic
  │
  ▼
proxy (kube-proxy)
  │
  ▼
Service (ClusterIP / VIP)
  │
  ├─── Pod
  ├─── Pod
  └─── Pod
        Kubernetes cluster
```

Service 支持的类型即 Kubernetes 中服务暴露的方式，默认四种：ClusterIP、NodePort、LoadBalancer、ExternalName，此外还有 Ingress。

---

### 01 ClusterIP — 默认类型，集群内访问

ClusterIP 类型的 Service 是 Kubernetes 集群默认的服务暴露方式，**只能用于集群内部通信**，可以被各 Pod 访问。

流量路径：
```
pod ---> ClusterIP:ServicePort ---> (iptables)DNAT ---> PodIP:containerPort
```

**演示：创建 Deployment + ClusterIP Service**

```yaml
# Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment-svc01
  labels:
    app: nginx-svc01
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nginx-svc01
  template:
    metadata:
      labels:
        app: nginx-svc01
    spec:
      containers:
      - name: nginx
        image: nginx:1.8
        ports:
        - containerPort: 80
---
# Service（不写 type 默认为 ClusterIP）
apiVersion: v1
kind: Service
metadata:
  name: nginx-svc01
spec:
  selector:
    app: nginx-svc01  # 选择我们上面创建 dep 中的 nginx pod
  ports:
  - protocol: TCP
    port: 8085         # service 对外暴露的端口
    targetPort: 80     # 代表目标容器的端口是 80
```

创建完后，`kubectl get svc -o wide` 可以看到 ClusterIP（如 `10.96.163.176`），在节点上 `curl 10.96.163.176:8085` 即可访问 nginx。集群外（如 master 节点 curl 后提示 connection refused）无法访问，符合预期。

---

### 02 NodePort — 节点端口对外暴露

NodePort 模式下，kube-proxy 在**每个节点**上打开一个指定端口（默认范围 30000-32767），集群外部可以通过任意节点 IP + NodePort 访问。

流量路径：
```
client ---> NodeIP:NodePort ---> ClusterIP:ServicePort ---> (iptables)DNAT ---> PodIP:containerPort
```

```
                  Service
                 /   |   \
               Pod  Pod  Pod
           Kubernetes cluster
          /         |        \
        VM          VM        VM
    port:30000  port:30000  port:30000
                  Traffic
```

**演示：修改 Service 类型为 NodePort**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nginx-svc01
spec:
  type: NodePort       # 此处是 NodePort 类型
  selector:
    app: nginx-svc01
  ports:
  - port: 8085
    protocol: TCP
    targetPort: 80
    nodePort: 30000    # 来自所有节点的 30000 端口请求都会路由到此
```

创建后，每个节点上的 kube-proxy 进程都会监听 8085 端口（`ss -nltp | grep 8085`）。在浏览器请求 `节点IP:8085` 也可访问，说明 NodePort 模式生效。

注意：需修改 apiserver 的 NodePort 端口范围配置：
```
vim /etc/kubernetes/manifests/kube-apiserver.yaml
  --service-node-port-range=1-20000
```

NodePort 模式下，ClusterIP 仍然可用（两种访问方式并存）。

---

### 03 LoadBalancer — 云厂商 LB 对外

LoadBalancer 类型的 Service 通常和云厂商的 LB 结合一起使用，用于将集群内部的服务暴露到外网。云厂商的 LoadBalancer 会给用户分配一个 IP，之后通过该 IP 的流量会转发到 Service 上。

```
        Traffic
           │
           ▼
    ┌─────────────────┐
    │  Load Balancer  │  ← 云厂商分配的外部 IP
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │    Service      │  ← Kubernetes cluster 内部
    └────────┬────────┘
          /  │  \
        Pod Pod Pod
```

---

### 04 ExternalName — CNAME 映射外部服务

通过 CNAME 将 Service 与 `externalName` 的值（比如 `foo.bar.example.com`）映射起来，这种方式用的比较少。

---

### 05 Ingress — Service 的 Service（七层路由）

Ingress 其实不是 Service 的一个类型，但它可以作用于多个 Service，被称为 "Service 的 Service"，作为集群内部服务的入口。Ingress 作用在七层，可以根据不同的 URL 将请求转发到不同的 Service 上。

```
          Traffic
             │
    ┌────────▼────────────────────────────┐
    │              Ingress                │
    │  foo.mydomain.com  mydomain.com/bar │
    └───────┬────────────────┬────────────┘
            │                │
         Service          Service
        /  |  \           /  |  \
      Pod Pod Pod       Pod Pod Pod
                  Kubernetes cluster
```

---

## §02 四种 Service 负载均衡模式

### 负载均衡模式概览

| 模式 | 转发由谁做 | 说明 |
|------|-----------|------|
| userspace | kube-proxy | 流量过内核+用户态，性能差，已淘汰 |
| iptables | iptables（netfilter） | 当前默认，基于 DNAT，不自动重试 |
| ipvs | ipvs（netfilter 钩子） | 哈希表，支持多种 LB 算法，大规模推荐 |
| kernelspace | Windows 内核 | 主要在 Windows 下使用 |

kube-proxy 负责维护上述规则。Service 的负载均衡实际上由 kube-proxy 在各节点上维护 iptables/ipvs 规则来实现。

**kube-proxy 版本历史**：
- Kubernetes v1.1：Ingress API 出现，同时以 "了解" 方式引入 iptables 代理
- Kubernetes v1.2：iptables 成为默认代理
- Kubernetes v1.8：引入 ipvs 代理（alpha）
- Kubernetes v1.9：ipvs 代理成为 beta
- Kubernetes v1.11：ipvs 代理正式 GA

---

### 01 userspace 模式 — 转发由 kube-proxy 做

```
        Node
  ┌────────────────────────────────────┐
  │  Client                apiserver  │
  │     │                      │      │
  │     ▼                      ▼      │
  │  clusterIP              kube-proxy│
  │  (iptables)─────────────────┘     │
  │     │                             │
  │  Backend Pod 1  Pod 2  Pod 3      │
  │  (port: 9376)                     │
  └────────────────────────────────────┘
```

在 userspace 模式下，访问服务的请求到达节点后首先进入内核 iptables，然后回到用户空间，由 kube-proxy 转发到后端 Pod。这样流量从用户空间进出内核带来的性能损耗是不可接受的，所以引入了 iptables 模式。

**为什么 userspace 模式要建立 iptables 规则？**  
因为 kube-proxy 监听的端口在用户空间，这个端口不是服务的访问端口也不是 nodePort，因此需要一层 iptables 把访问服务的连接重定向给 kube-proxy 服务。

---

### 02 iptables 模式 — 转发由 iptables 做（当前默认）

```
              apiserver
                  │
  ┌───────────────▼──────────────────┐
  │  Client      kube-proxy          │
  │     │            │               │
  │     ▼            ▼               │
  │  clusterIP (iptables rules)  Node│
  │     │                            │
  │  Pod 1     Pod 2     Pod 3       │
  └────────────────────────────────── ┘
```

iptables 模式是目前默认的代理方式，基于 netfilter 实现。当客户端请求 Service 的 ClusterIP 时，根据 iptables 规则路由到各 Pod 上，iptables 使用 DNAT 来完成转发，其采用了随机数实现负载均衡。

**iptables 与 userspace 模式区别**：
- 最大的区别在于 iptables 模块使用 DNAT 实现了 Service 入口地址到 Pod 实际地址的转换，免去了一次内核态到用户态的切换
- 使用 iptables 处理流量具有较低的系统开销，流量由 Linux netfilter 处理，无需在用户空间和内核空间之间切换
- 与 userspace 代理不同：如果 iptables 代理最初选择的那个 Pod 没有响应，它不会自动重试其他 Pod（userspace 会）

**iptables 的问题**：iptables 规则的数量与 Service 数量成正比，对于生产集群来讲，大量的 iptables 规则会占用大量内存，不能进行增量更新，对路由规则的增删改都要遍历一次链表。

---

### 03 ipvs 模式 — 转发由 ipvs 做（大规模推荐）

ipvs 也实现在 netfilter 的钩子函数上，但使用哈希表作为底层的数据结构并且工作在内核空间，意味着 ipvs 可以更快地重定向流量，并且在同步代理规则时具有更好的性能。

**ipvs 支持三种代理模式**：
- DR：Direct Routing（直接路由）
- NAT：Network Address Translation（网络地址转换）
- Tunneling（也叫 ipip 模式）

**ipvs 还支持更多的负载均衡算法**：
- `rr`：round-robin / 轮询
- `lc`：least connection / 最少连接
- `dh`：destination hashing / 目标哈希
- `sh`：source hashing / 源哈希
- `sed`：shortest expected delay / 预计延迟时间最短
- `nq`：never queue / 从不排队

> userspace、iptables、ipvs 三种模式中默认的负载均衡策略都是通过 round-robin 算法来选择后端 Pod。

**ipvs 比 iptables 的优点**：
- ipvs 和 iptables 的实现都基于 netfilter 的钩子函数
- iptables 使用链表，对路由规则的增删改都要遍历一次链表
- ipvs 使用哈希表作为底层的数据结构并且工作在内核，速度快

---

### 04 kernelspace 模式 — 主要在 Windows 下使用

kernelspace 模式主要在 Windows 节点下使用，不展开。

---

## §03 两种服务发现模式

Service 当前支持两种类型的服务发现机制：**环境变量**和 **DNS**。推荐使用后者，因为使用环境变量有 svc 和 pod 顺序问题——必须在客户端 Pod 出现之前创建服务，否则这些客户端 Pod 将不会设定其环境变量。

---

### 01 环境变量

当 Pod 运行在 Node 上，kubelet 会为每个活跃的 Service 添加一组环境变量（遵循 Docker Links 变量）。

以 nginx-svc01 为例，Service 暴露了 TCP 端口 8085，同时给它分配了 Cluster IP 地址 `10.96.203.90`，这个 Service 会生成如下环境变量：

```
NGINX_SVC01_PORT_8085_TCP_ADDR=10.96.203.90
NGINX_SVC01_SERVICE_PORT=8085
NGINX_SVC01_PORT=tcp://10.96.203.90:8085
NGINX_SVC01_PORT_8085_TCP_PORT=8085
```

Service 创建比 Pod 晚时，Pod 中不会有该 Service 的环境变量（这是环境变量方式的核心缺陷）。

---

### 02 DNS（推荐）

可以在集群中部署 CoreDNS 服务（安装 Kubernetes 的时候默认部署），使用 DNS 作为服务发现。k8s 会为 Service 创建 cordns 解析：

- 解析域名为 `${service_name}.${namespace}.svc.cluster.local`
- 其中 `cluster.local` 代表集群的后缀
- 如 nginx-svc01 的域名为 `nginx-svc01.default.svc.cluster.local`

**Pod 中的三个 DNS 搜索域**（`/etc/resolv.conf`）：

```
search default.svc.cluster.local svc.cluster.local cluster.local
nameserver 10.96.0.10
options ndots:5
```

Pod 访问短域名时，会依次补全这三个搜索域，所以 `ping nginx-svc01`、`ping nginx-svc01.default`、`ping nginx-svc01.default.svc.cluster.local` 都能解析到同一个 IP。

**Pod 中验证 DNS**（部署一个带 curl 命令的 Pod）：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment-curl01
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx-curl01
  template:
    metadata:
      labels:
        app: nginx-curl01
    spec:
      containers:
      - name: nginx
        image: yauritux/busybox-curl
        ports:
        - containerPort: 80
        command:
        - sleep
        - "3600"
```

在容器内 `ping nginx-svc01` 可以看到解析到的 IP 地址（10.96.203.90），`curl nginx-svc01.default.svc.cluster.local:8085` 可以访问到 nginx。

**Node 上的 DNS 验证**：

Node 上的 `/etc/resolv.conf` 没有配置集群相关的搜索域，所以必须要写全域名 FQDN，且必须 dig @CoreDNS 的 IP 才可以：

```bash
# 先获取 CoreDNS 的 ClusterIP
kubectl get svc -n kube-system | grep dns
# kube-dns  ClusterIP  10.96.0.10  53/UDP,53/TCP,9153/TCP

# 指定 CoreDNS 解析
dig nginx-svc01.default.svc.cluster.local @10.96.0.10
# 返回 ANSWER: 1，解析到 ClusterIP
```

然后 `curl 10.96.203.90:8085` 即可访问。

---

### Headless Service — StatefulSet 的基础设施

**无头服务（Headless Service）**：`spec.clusterIP: None`，不分配 ClusterIP，DNS 配置返回 A 记录（IP 地址），通过这个地址直接到达 Service 的后端 Pod 上。

- 带选择算符的服务：Endpoint 控制器在 API 中创建了 Endpoints 记录，并且修改 DNS 配置返回 A 记录（IP 地址），通过这个地址直接到达 Service 的后端 Pod 上
- 无选择算符的服务：Endpoint 控制器不会创建 Endpoints 记录

当 scale 扩容 Deployment 后，Headless Service 域名的 DNS a 记录会被修改为多个 Pod IP 地址。

**创建带标签的无头服务**：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: nginx-svc02
spec:
  clusterIP: None       # 关键配置
  ports:
  - port: 8086
    protocol: TCP
    targetPort: 80
  selector:
    app: nginx-svc01
```

创建完后，`kubectl get svc` 可以看到 nginx-svc02 的 CLUSTER-IP 列为 `None`，确认无 ClusterIP。

dig 这个无头服务的域名返回的 A 记录**直接指向后端 Pod IP**：
```bash
dig nginx-svc02.default.svc.cluster.local @10.96.0.10
# ANSWER SECTION:
# nginx-svc02.default.svc.cluster.local. 30 IN A 10.100.85.228
```

直接 `curl 10.100.85.228` (80 端口) 就能访问到 Pod。

---

### Headless Service 为 StatefulSet 的实际应用场景

StatefulSet 当前需要无头服务来负责 Pod 的网络标识，原因如下：

像 Elasticsearch、etcd 这种分布式服务，在集群初期 setup 时，配置文件中就要写上集群中所有节点的 IP（或是域名）。如果使用普通 Service 的话，无论多少个 Pod 都只有同一个 VIP，没有办法区分各个 Pod。而无头服务为 StatefulSet 提供一组服务内部区别各个 Pod 的手段——每个 Pod 都有唯一的 DNS 域名：

```
{pod-name}.{headless-svc}.{namespace}.svc.cluster.local
```

**ES 配置示例**：

```yaml
# 以 es 为例
node.name: es-01
network.host: 0.0.0.0
http.port: 9200
transport.tcp.port: 9300
cluster.initial_master_nodes:
  - es-01
  - es-02
  - es-03
discovery.seed_hosts:
  - 192.168.80.1:9300   # 静态 IP 方式（不推荐）
  - 192.168.80.2:9300
  - 192.168.80.3:9300
```

如果用无头服务 + StatefulSet，就可以写成：

```yaml
discovery.seed_hosts:
  - web-0.nginx-sts01.default.svc.cluster.local:9300
  - web-1.nginx-sts01.default.svc.cluster.local:9300
  - web-2.nginx-sts01.default.svc.cluster.local:9300
```

**演示验证**：

```yaml
# 无头 Service + StatefulSet
apiVersion: v1
kind: Service
metadata:
  name: nginx-sts01
  labels:
    app: nginx-sts01
spec:
  ports:
  - port: 80
    name: web
  clusterIP: None
  selector:
    app: nginx-sts01
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web
spec:
  serviceName: "nginx-sts01"
  replicas: 2
  selector:
    matchLabels:
      app: nginx-sts01
  template:
    metadata:
      labels:
        app: nginx-sts01
    spec:
      containers:
      - name: nginx
        image: nginx:1.7.9
        ports:
        - containerPort: 80
          name: web
```

创建完后，Pod 为 `web-0`（10.100.85.234）和 `web-1`（10.100.85.233）。

dig 无头服务域名返回两个 Pod 的 A 记录：
```bash
dig nginx-sts01.default.svc.cluster.local @10.96.0.10
# ANSWER SECTION:
# nginx-sts01.default.svc.cluster.local. IN A 10.100.85.234
# nginx-sts01.default.svc.cluster.local. IN A 10.100.85.233
```

还可以 dig 单个 Pod 的域名，精确路由到指定 Pod：
```bash
dig web-0.nginx-sts01.default.svc.cluster.local @10.96.0.10
# 只返回 web-0 的 IP：10.100.85.234
dig web-1.nginx-sts01.default.svc.cluster.local @10.96.0.10
# 只返回 web-1 的 IP：10.100.85.233
```

如果不知道 etcd 配置文件中域名后 port 怎么写，可以使用 `pod-序号+sts对应的无头服务的域名+port` 的格式（例如 `web-0.nginx-sts01.default.svc.cluster.local:9300`）。
