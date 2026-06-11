# 第29章 kube-proxy iptables 和 ipvs 模式源码解读

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 29 章 — kube-proxy iptables 和 ipvs 模式源码解读
> **源码入口**: `cmd/kube-proxy/app/server.go:641`（`Run`）、`pkg/proxy/iptables/proxier.go:810`（`syncProxyRules`）、`pkg/proxy/ipvs/proxier.go:1028`（`syncProxyRules`）

---

## 核心机制一览

1. **Proxier 接口隔离四种模式**：kube-proxy 定义 `Provider` 接口，`iptables.Proxier` / `ipvs.Proxier` / `userspace.Proxier` / `winkernel.Proxier` 分别实现。`NewProxyServer` 根据配置中的 `proxyMode` 分支构造对应的实现类并赋值给 `s.Proxier`，后续所有调用都通过接口，做到模式无感知切换。

2. **Run 是所有模式的统一入口**：`ProxyServer.Run`（`:641`）负责：设置 OOM score → 启动 Event 记录 → 初始化 healthz 服务 → 启动 metrics 服务 → 配置 conntrack 内核参数 → 设置标签选择器 → 启动 informer 监听 Service + Endpoints/EndpointSlices → 触发第一次全量同步（`OnServiceSynced` → `syncProxyRules`）→ 启动周期 sync loop（`s.Proxier.SyncLoop`）。

3. **syncProxyRules 是核心**：iptables 和 ipvs 各自实现 `syncProxyRules`，是规则写入内核的入口。触发时机有两个：全量同步（informer 初次同步完后 `OnServiceSynced` 调一次）和周期同步（`BoundedFrequencyRunner` 的 `SyncLoop` 定期调用）。

4. **iptables 模式：七条自定义链 + 每 Service 两条链**：kube-proxy 自定义 `KUBE-SERVICES`、`KUBE-EXTERNAL-SERVICES`、`KUBE-NODEPORTS`、`KUBE-POSTROUTING`、`KUBE-MARK-MASQ`、`KUBE-MARK-DROP`、`KUBE-FORWARD` 七条固定链；每个 Service 额外创建 `KUBE-SVC-xxx`（Service portal 链）和 `KUBE-SEP-xxx`（per-endpoint DNAT 链）。流量路径：`PREROUTING → KUBE-SERVICES → KUBE-SVC-xxx → KUBE-SEP-xxx → DNAT`。

5. **ipvs 模式：规则数量恒定，不随 Service/Pod 数增长**：ipvs 用哈希表存虚拟服务器，kube-proxy 在每个节点上创建一个 dummy 网卡 `kube-ipvs0` 并把所有 Service ClusterIP 绑上去，流量命中 LOCAL_IN 后由 ipvs 做 NAT 转发。iptables 规则数量固定（只剩 MASQUERADE 等辅助规则），规则创建先写入 map 然后批量 flush 一次写入内核。

---

## 全章调用链总图

```
main (cmd/kube-proxy/main.go)
  │
  ▼ cobra 路由
ProxyServer.Run (server.go:641)
  │
  ├─── 设置 OOM score (-999)
  ├─── 启动 Event 记录 (s.Broadcaster.StartRecordingToSink)
  ├─── 启动 healthz 服务 (serveHealthz)
  │       └── 检测 lastUpdated / lastQueued 时间戳
  ├─── 启动 metrics 服务 (serveMetrics)
  ├─── 配置 conntrack 内核参数
  ├─── 设置标签 (noProxyName / noHeadlessEndpoints)
  ├─── 启动 informerFactory 监听 Service + Endpoints/EndpointSlices
  ├─── serviceConfig / endpointsConfig 第一次全量同步
  │       └── OnServiceSynced → proxier.syncProxyRules()  ← 首次写规则
  └─── s.Proxier.SyncLoop()  ← 周期循环
          │
          └── BoundedFrequencyRunner.Loop
                └── tryRun → limiter → bfr.fn = proxier.syncProxyRules
                                            │
                      ┌─────────────────────┴──────────────────────┐
                      │ iptables                                    │ ipvs
                      ▼                                            ▼
          pkg/proxy/iptables/proxier.go:810          pkg/proxy/ipvs/proxier.go:1028
          syncProxyRules()                           syncProxyRules()
```

---

## §01 启动流程：判断代理模式，初始化 Proxier 接口

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| Run 主流程 | [server.go](kubernetes/cmd/kube-proxy/app/server.go) | `Run:641` |
| iptables Proxier 构造 | [proxier.go](kubernetes/pkg/proxy/iptables/proxier.go) | `NewProxier:253` |
| ipvs Proxier 构造 | [proxier.go](kubernetes/pkg/proxy/ipvs/proxier.go) | `NewProxier:345` |
| BoundedFrequencyRunner | [bounded_frequency_runner.go](kubernetes/pkg/util/async/bounded_frequency_runner.go) | `NewBoundedFrequencyRunner:155` |

### Run() 初始化流程详解

```
ProxyServer.Run (server.go:641)
  │
  ▼ 设置 OOM score
  │  oomAdjuster = oom.NewOOMAdjuster()
  │  oomAdjuster.ApplyOOMScoreAdj(0, s.OOMScoreAdj)
  │  KubeProxyOOMScoreAdj = -999  (pkg/kubelet/qos/policy.go)
  │
  ▼ 设置 Event 记录
  │  s.Broadcaster.StartRecordingToSink(stopCh)
  │
  ▼ 初始化 healthz 服务 (serveHealthz)
  │  检测 lastUpdated / lastQueued 时间戳判断是否健康
  │  lastUpdated.IsZero() → healthy = true (proxy 启动中)
  │  lastUpdated.After(lastQueued) → healthy = true (已处理完所有更新)
  │  currentTime.Sub(lastQueued) < healthTimeout → healthy = true
  │
  ▼ 开启 metrics 服务 (serveMetrics)
  │  proxyMux.HandleFunc("/proxyMode", ...) → 返回当前代理模式文本
  │  proxyMux.Handle("/metrics", legacyregistry.Handler())
  │
  ▼ 配置 conntrack 内核参数
  │  nf_conntrack_tcp_timeout_established
  │  nf_conntrack_tcp_timeout_close_wait
  │
  ▼ 设置标签选择器 (过滤无需代理的 Service)
  │  noProxyName: LabelServiceProxyName DoesNotExist
  │  noHeadlessEndpoints: IsHeadlessService DoesNotExist
  │
  ▼ 启动 informerFactory (监听 Service / Endpoints / EndpointSlices)
  │  informerFactory.WithTweakListOptions → 过滤 labelSelector
  │
  ▼ serviceConfig.Run — 第一次全量同步
  │  serviceConfig.RegisterEventHandler(s.Proxier)
  │  OnServiceSynced → proxier.syncProxyRules()
  │
  ▼ endpointSliceConfig / endpointsConfig.Run — 第一次全量同步
  │  (UseEndpointSlices 分支)
  │  对应 Run 和上面 svc 一样，都是 syncProxyRules
  │
  ▼ informerFactory.Start(wait.NeverStop)
  │
  └─ go s.Proxier.SyncLoop()
```

**为什么引入 EndpointSlice？**  
Endpoints 性能不好：每次一个 Pod Ready/NotReady，整个 Endpoints 对象都要重新下发给所有节点；EndpointSlice 分片存储，变更只触发相关分片的下发，大规模集群下显著减少 apiserver 压力。

### SyncLoop 与 BoundedFrequencyRunner

```go
// pkg/proxy/iptables/proxier.go:529
func (proxier *Proxier) SyncLoop() {
    proxier.syncRunner.Loop(wait.NeverStop)
}
```

`syncRunner` 是一个 `BoundedFrequencyRunner`，在 `NewProxier` 时构造：

```go
proxier.syncRunner = async.NewBoundedFrequencyRunner(
    "sync-runner", proxier.syncProxyRules,
    minSyncPeriod, syncPeriod, burstSyncs,
)
```

`BoundedFrequencyRunner.Loop` 内部是一个 `for { select }` 循环，监听三个 channel：
- `<-stop`：停止
- `<-bfr.timer.C`：定时器到期触发
- `<-bfr.run`：手动触发（OnServiceSynced 等事件）

`tryRun` 中先查询 limiter 是否允许，允许则执行 `bfr.fn`（即 `syncProxyRules`），否则设置下一次执行时间。这保证了在事件风暴期间不会无限触发规则同步。

---

## §02 Proxier 运行：Run 完整流程梳理

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| SyncLoop | [proxier.go](kubernetes/pkg/proxy/iptables/proxier.go) | `SyncLoop:529` |
| OnServiceSynced | [proxier.go](kubernetes/pkg/proxy/iptables/proxier.go) | `OnServiceSynced:575` |

### 本节重点

- `Run()` 调用了 `NewProxyServer()` 来初始化 proxyServer 对象，其中包括初始化每种模式对应的 proxier，该方法最终会调用 `s.Proxier.SyncLoop()` 执行 proxier 的主循环。
- 同时启动 serviceConfig 和 endpoint/endpointSlice 的第一次全量同步。
- 可以看到全量同步和 Loop 底层调用的方法都是 `proxier.syncProxyRules`。

---

## §03 iptables 模式规则分析

### iptables 基础回顾

iptables 五张表（优先级从高到低：raw、mangle、nat、filter、security）；四条内置链 PREROUTING / INPUT / FORWARD / OUTPUT / POSTROUTING。

**流量路径（完整 iptables Flowchart）**：

```
Incoming Packet
  │
  ▼ raw PREROUTING → Connection Tracking → mangle PREROUTING
  │
  ├── localhost source? Y → mangle INPUT → filter INPUT → Local Processing
  │                                                           │
  │                                               mangle OUTPUT → nat OUTPUT
  │                                               → Routing Decision → filter OUTPUT
  │                                               → (mangle POSTROUTING → nat POSTROUTING)
  │                                               → Outgoing Packet
  │
  └── N → nat PREROUTING → Routing Decision
              │
              ├── For this host? Y → mangle INPUT → security INPUT → nat INPUT → Local Processing
              │
              └── N → mangle FORWARD → filter FORWARD → security FORWARD
                          → Release to Outbound Interface
                          → mangle POSTROUTING → localhost dest?
                              Y → nat POSTROUTING → Outgoing
                              N → nat POSTROUTING → Outgoing
```

### kube-proxy iptables 模式自定义链

kube-proxy 使用 filter 表和 nat 表，自定义七条固定链：

| 链名 | 所在表 | 用途 |
|------|-------|------|
| KUBE-SERVICES | nat | 入口链，所有入站流量先进这里 |
| KUBE-EXTERNAL-SERVICES | filter | 处理外部服务 |
| KUBE-NODEPORTS | nat | NodePort 流量入口 |
| KUBE-POSTROUTING | nat | MASQUERADE 处理 |
| KUBE-MARK-MASQ | nat | 打 0x4000 标记（需要 MASQUERADE） |
| KUBE-MARK-DROP | nat | 打 0x8000 标记（需要 DROP） |
| KUBE-FORWARD | filter | 转发规则 |

每个 Service 额外创建：
- `KUBE-SVC-xxx`：Service portal 链，负责分流到各 endpoint
- `KUBE-SEP-xxx`（per endpoint）：负责 DNAT 到具体 Pod IP:Port

**nat 表中的链结构**：

```
PREROUTING → KUBE-SERVICES (kubernetes service portals)
OUTPUT     → KUBE-SERVICES (kubernetes service portals)
POSTROUTING → KUBE-POSTROUTING (kubernetes postrouting)

KUBE-SERVICES
  ├─── KUBE-MARK-MASQ (source != podIP && destination == clusterIP)
  ├─── KUBE-SVC-xxx   (destination == clusterIP)         R2
  ├─── KUBE-NODEPORTS                                     last rule
  └─── KUBE-EXTERNAL-SERVICES / KUBE-FW-xxx              R3: dest == LB IP

KUBE-SVC-xxx
  └─── KUBE-SEP-xxx (statistic mode random → DNAT to podIP:port)

KUBE-POSTROUTING
  ├─── RETURN     (mark match !0x4000/0x4000)
  ├─── MARK xor   (clear mark bit)
  └─── MASQUERADE (kubernetes service traffic requiring SNAT)
```

### ClusterIP 访问链路

数据包流向（从 Pod 内访问 ClusterIP）：

```
PREROUTING --> KUBE-SERVICES --> KUBE-SVC-xxx --> KUBE-SEP-xxx
```

具体分析（以 nginx-svc05 ClusterIP=10.96.112.4 为例）：

1. 进入 `PREROUTING` 转到 `KUBE-SERVICES`
2. 在 `KUBE-SERVICES`，目的为 ClusterIP 10.96.112.4 → 匹配到 `KUBE-SVC-WZ2BDG775GVNWW7K`
3. `KUBE-SVC-xxx` 用 `--mode random` 随机选择一个 `KUBE-SEP-xxx`
4. `KUBE-SEP-xxx` 先 `KUBE-MARK-MASQ`（源是 Pod 自己的情况），再 `DNAT → podIP:port`

### NodePort 访问链路

NodePort 的 KUBE-NODEPORTS 链位于 KUBE-SERVICES 的最后一条规则，所以只有前面所有 ClusterIP 规则都不匹配时才会走到 NodePort。

```
# 非本机访问
PREROUTING --> KUBE-SERVICE --> KUBE-NODEPORTS --> KUBE-SVC-xxx --> KUBE-SEP-xxx

# 本机访问
OUTPUT --> KUBE-SERVICE --> KUBE-NODEPORTS --> KUBE-SVC-xxx --> KUBE-SEP-xxx
```

---

## §04 iptables 模式 syncProxyRules 解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| syncProxyRules（iptables） | [proxier.go](kubernetes/pkg/proxy/iptables/proxier.go) | `syncProxyRules:810` |

### 整体步骤

```go
// pkg/proxy/iptables/proxier.go:810
func (proxier *Proxier) syncProxyRules() {
    // 1. 更新 serviceMap / endpointsMap
    serviceUpdateResult = proxier.serviceMap.Update(proxier.serviceChanges)
    endpointUpdateResult = proxier.endpointsMap.Update(proxier.endpointsChanges)

    // 2. 初始化 iptables 链（目标是 filter 和 nat 两张表的 buffer 中）
    //    先获取所有现有链/规则 (iptables-save)，
    //    若链已存在则 Write 进 buffer；若不存在则新建
    for _, jump := range iptablesJumpChain {
        proxier.iptables.EnsureChain(jump.table, jump.dstChain)
    }
    // 所有规则写入 filterChains / filterRules / natChains / natRules buffer

    // 3. 写入 table header（filter / nat 表头）
    proxier.filterChains.Reset()
    proxier.filterRules.Reset()
    proxier.natChains.Reset()
    proxier.natRules.Reset()
    utilproxy.WriteLine(proxier.filterChains, "*filter")
    utilproxy.WriteLine(proxier.natChains, "*nat")

    // 4. 确保七条固定链存在（写入 buffer）
    for _, chainName := range []utiliptables.Chain{...} {
        if chain, ok := existingNatChains[chainName]; ok {
            utilproxy.WriteBytesLine(proxier.natChains, chain)
        } else {
            utilproxy.WriteLine(proxier.natChains, utiliptables.MakeChainLine(chainName))
        }
    }

    // 5. 写 SNAT 规则（KUBE-POSTROUTING 中的 MASQUERADE）
    //    为出站 Pod 流量在 POSTROUTING 中打 MASQUERADE，
    //    用 mark match 只对 0x4000 标记的包做 MASQUERADE

    // 6. 为每个 Service 创建规则
    for svcName, svc := range proxier.serviceMap {
        // 创建 KUBE-SVC-xxx 和 KUBE-XLB-xxx 链
        // 写 service portal 规则（ClusterIP 匹配）
        // 写 externalIP 规则
        // 写 ingress/LB 规则
        // 写 nodePort 规则

        // 为每个 endpoint 写 KUBE-SEP-xxx 链
        // 按三个桶分类: readyEndpoints / localReadyEndpoints / localServingTerminatingEndpoints
        // 写 DNAT 规则
    }

    // 7. 在 KUBE-SERVICES 末尾追加 nodePort 规则（指向 KUBE-NODEPORTS）

    // 8. 删除不再使用的 KUBE-SVC-xxx / KUBE-SEP-xxx 链

    // 9. 使用 iptables-restore 一次性写入（NoFlushTables 不清空非 kube 链）
    proxier.iptables.RestoreAll(proxier.natRules.Bytes(), ...)
}
```

**关键设计**：
- 所有规则先写入内存 buffer（`natChains`、`natRules`、`filterChains`、`filterRules`），最后一次 `iptables-restore` 批量刷入内核，避免逐条调用 iptables 命令的开销。
- `NoFlushTables` 模式：只更新 kube 自定义链，不 flush 整张表（保留其他进程写的规则）。
- endpoint 按三个桶分类：ready endpoints、local ready endpoints、local serving terminating endpoints。对于 `ExternalTrafficPolicy: Local` 的 Service，只路由到本节点的 endpoints。

---

## §05 ipvs 模式 syncProxyRules 解析

| 读码目标 | 源文件（可点击） | 入口函数 |
|---------|----------------|---------|
| syncProxyRules（ipvs） | [proxier.go](kubernetes/pkg/proxy/ipvs/proxier.go) | `syncProxyRules:1028` |
| SyncLoop（ipvs） | [proxier.go](kubernetes/pkg/proxy/ipvs/proxier.go) | `SyncLoop:848` |

### ipvs 三种代理模式

ipvs（IP Virtual Server）基于 netfilter，实现在内核中：

| 模式 | 全称 | 特点 |
|------|------|------|
| NAT | Network Address Translation | 支持端口映射，Kubernetes 使用此模式 |
| TUN | IP Tunneling | 跨节点隧道，不支持端口映射 |
| DR | Direct Routing | 直接路由，性能最高，不支持端口映射 |

Kubernetes 使用 NAT 模式，因为需要将 service IP:port 映射到 container IP:container port。

**ipvs NAT 模式数据流**：

```
Client
  │ send to VIP (ClusterIP)
  │
  ▼
Director (Node running ipvs)
  │ DNAT: VIP → Real Server IP
  ▼
Real Server (Pod)
  │ reply to Client IP
  ▼
Director (reply also goes through Director, SNAT back)
  │
  ▼
Client
```

### ipvs 与 iptables 区别

| 维度 | iptables | ipvs |
|------|----------|------|
| 底层数据结构 | 链表 | 哈希表 |
| 规则查找复杂度 | O(N)，N=Service数 | O(1) |
| 支持的 LB 算法 | rr（随机模拟）、lc | rr / lc / dh / sh / sed / nq 共 8 种 |
| 规则数量随 Service 增长 | 线性增长 | 固定（iptables 规则固定，ipvs 虚拟服务器哈希表增长） |

**ipvs 模式下 kube-proxy 自定义八条链**（比 iptables 多一条 KUBE-LOAD-BALANCER）：
- KUBE-SERVICES、KUBE-FIREWALL、KUBE-POSTROUTING、KUBE-MARK-MASQ、KUBE-NODE-PORT、KUBE-MARK-DROP、KUBE-FORWARD、KUBE-LOAD-BALANCER

### ipvs 模式数据包流向（ClusterIP 访问）

```
PREROUTING --> KUBE-SERVICES --> KUBE-CLUSTER-IP --> INPUT --> KUBE-FIREWALL --> POSTROUTING
```

- 进入 PREROUTING 链
- 从 PREROUTING 转到 KUBE-SERVICES，10.244.0.0/16 为 ClusterIP 网段
- 在 KUBE-SERVICES 链打标记，再进入 KUBE-CLUSTER-IP 链
- KUBE-CLUSTER-IP 为 ipset 集合，在此会进行 DNAT
- 进入 INPUT 链后转到 KUBE-FIREWALL 链检查标记
- ipvs 的 LOCAL_IN Hook 发现此包在 ipvs 规则中则直接转发到 POSTROUTING 链

### syncProxyRules（ipvs）步骤

```go
// pkg/proxy/ipvs/proxier.go:1028
func (proxier *Proxier) syncProxyRules() {
    // 1. 更新 serviceMap / endpointsMap
    serviceUpdateResult = proxier.serviceMap.Update(proxier.serviceChanges)
    endpointUpdateResult = proxier.endpointsMap.Update(proxier.endpointsChanges)

    // 2. 初始化 iptables（ipvs 模式下 iptables 仍然存在，只是规则少）
    //    创建 dummy 网卡 kube-ipvs0，确保存在
    proxier.netlinkHandle.EnsureDummyDevice(DefaultDummyDevice)

    // 3. 确保所有 ipset 存在（KUBE-CLUSTER-IP、KUBE-NODE-PORT 等 ipset）
    for _, set := range proxier.ipsetList {
        ensureIPSet(set)
        set.resetEntries()
    }

    // 4. 对每一个服务创建 ipvs 规则
    for svcName, svc := range proxier.serviceMap {
        // 对于 clusterIP 类型：更新 KUBE-CLUSTER-IP ipset
        entry := &utilipset.Entry{IP: svcInfo.ClusterIP().String(), Port: svcInfo.Port(), ...}
        proxier.ipsetList[kubeClusterIPSet].activeEntries.Insert(entry.String())

        // ipvs call：创建/更新虚拟服务器
        serv := &utilipvs.VirtualServer{
            Address:   netutils.BigForInt(svcInfo.ClusterIP()),
            Port:      uint16(svcInfo.Port()),
            Protocol:  strings.ToLower(svcInfo.Protocol()),
            Scheduler: proxier.ipvsScheduler,
        }
        proxier.syncService(svcNameString, serv, ...)

        // 对于 externalIP 类型：更新对应 ipset
        // 对于 load-balancer：更新 KUBE-LOAD-BALANCER ipset
        // 对于 nodePort：更新 KUBE-NODE-PORT ipset

        // 对 endpoint 添加 real server
        for _, ep := range proxier.endpointsMap[svcName] {
            proxier.syncEndpoint(svcName, svcInfo.ExternalPolicyLocal(), serv)
        }
    }

    // 5. 同步 ipset 记录（把 activeEntries 写入内核 ipset）
    for _, set := range proxier.ipsetList {
        set.syncIPSetEntries()
    }

    // 6. 写 iptables 规则（数量固定，只做 MASQUERADE 等辅助）
    proxier.writeIptablesRules()

    // 7. 批量写入 iptables（NoFlushTables）
    proxier.iptablesData.Reset()
    proxier.iptablesData.Write(proxier.natChains.Bytes())
    proxier.iptablesData.Write(proxier.natRules.Bytes())
    proxier.iptablesData.Write(proxier.filterChains.Bytes())
    proxier.iptablesData.Write(proxier.filterRules.Bytes())
}
```

**关键设计**：
- **规则数量恒定**：无论有多少 pod/service，iptables 的规则数都是固定的。只有 ipvs 的虚拟服务器哈希表随 Service 数量增长，但哈希表查找是 O(1)，不影响性能。
- **先写 map 再批量写入**：创建规则先插入 map（activeEntries），然后批量一次性 flush 进内核，避免频繁系统调用。
- **dummy 网卡 kube-ipvs0**：把所有 Service ClusterIP 都绑定到这个虚拟网卡上，使得内核认为这些 IP 是本机 IP，流量命中 LOCAL_IN 而非 FORWARD，ipvs 才能接管。
