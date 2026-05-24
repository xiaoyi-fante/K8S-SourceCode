# 第02章 创建pod时kubectl的执行流程和它的设计模式

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第02章 — kubectl 执行流程与设计模式
> **源码入口**: `cmd/kubectl/main.go`

---

## 核心机制一览

1. **kubectl 的本质**：kubectl 不做业务逻辑，只做一件事——把用户提交的内容（命令行参数或 yaml 文件）组织成标准数据结构，然后发送给 API Server。

2. **cobra 命令树**：kubectl 的所有子命令（create、get、apply 等）注册为一棵 cobra.Command 树，入口统一，路由清晰。

3. **Builder 建造者模式**：create 命令执行时，用 Builder 链式收集配置（文件路径、资源类型、namespace 等），最终调用 `Do()` 生成可执行的 Result。

4. **Visitor 访问者模式**：Builder 产出的 Result 是一组 Visitor 的组合，每个 Visitor 负责一件事（加载文件、校验 schema、填充默认值、发送请求），彼此解耦，按顺序执行。

---

## 01. 使用kubectl部署一个简单的nginx pod

### 本节学习目标

从创建 pod 的全流程入手，了解各组件的工作内容：

- **kubectl** — 接收用户指令，组织数据，发送给 API Server
- **kube-apiserver** — 接收请求，鉴权，持久化到 etcd
- **etcd** — 存储集群状态
- **kube-scheduler** — 监听未调度的 pod，选择合适节点
- **kube-controller** — 维护资源的期望状态
- **kubelet** — 在节点上真正拉起容器

### 编写 nginx pod 的 yaml

```yaml
# nginx_pod.yaml
apiVersion: v1
kind: Pod
metadata:
  name: nginx-pod
spec:
  containers:
  - name: nginx
    image: nginx:1.8
```

### 部署并观察状态

```bash
# 创建 pod
kubectl create -f nginx_pod.yaml
# pod/nginx-pod created

# 观察状态
kubectl get pod
```

输出示例：

```
NAME        READY   STATUS    RESTARTS   AGE
nginx-pod   1/1     Running   0          92s
```

各字段含义：

| 字段 | 含义 |
|------|------|
| NAME | 对应 yaml 中 `metadata.name` |
| READY | 就绪的容器数 / 总容器数 |
| STATUS | 当前状态，Running 表示运行中 |
| RESTARTS | 重启次数，0 代表未曾重启 |
| AGE | pod 运行时长 |

---

## 02. 命令行解析工具cobra的使用

### kubectl 的职责

kubectl 的工作分三步：

```
用户输入（命令行参数 / yaml 文件）
  │
  ▼ 解析 & 组织成数据结构体
  │
  ▼ 发送给 API Server
```

这三步看起来简单，但"解析 & 组织"这一步涉及大量细节：命令路由、参数校验、资源类型识别、schema 填充……cobra 库负责解决命令路由这一层。

### cobra 的核心概念

cobra 是 Go 生态中最主流的 CLI 框架（kubectl、hugo、docker 都用它）。核心对象是 `cobra.Command`：

```go
// vendor/github.com/spf13/cobra/command.go
type Command struct {
    Use   string          // 命令名，如 "create"
    Short string          // 简短说明
    RunE  func(cmd *Command, args []string) error  // 实际执行函数
    // ...
}
```

kubectl 把所有子命令组织成一棵树：

```
kubectl (root Command)
  ├── create
  │     ├── -f <file>
  │     └── deployment / pod / service ...
  ├── get
  ├── apply
  ├── delete
  └── ...（共7大命令分组）
```

cobra 负责：根据用户输入的字符串，找到对应的 `Command` 节点，调用其 `RunE` 函数。

---

## 03. kubectl命令行设置pprof抓取火焰图

> 待补充截图内容

---

## 04. kubectl命令行设置7大命令分组

> 待补充截图内容

---

## 05. create命令执行流程

> 待补充截图内容

---

## 06. createCmd中的builder建造者设计模式

> 待补充截图内容

---

## 07. createCmd中的visitor访问者设计模式

> 待补充截图内容

---

## 08. kubectl功能和对象总结

> 待补充截图内容
