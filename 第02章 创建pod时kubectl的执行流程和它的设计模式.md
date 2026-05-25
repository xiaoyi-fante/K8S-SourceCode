# 第02章 创建pod时kubectl的执行流程和它的设计模式

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第02章 — kubectl 执行流程与设计模式
> **源码入口**: `cmd/kubectl/main.go`

---

## 核心机制一览

1. **kubectl 的本质**：kubectl 不做业务逻辑，只做一件事——把用户提交的内容（命令行参数或 yaml 文件）组织成标准数据结构，然后发送给 API Server。整个实现分三步：cobra 解析命令 → Builder/Visitor 组装资源 → RESTClient 发送请求。

2. **cobra 命令树与生命周期钩子**：所有子命令注册为一棵 cobra.Command 树，路由清晰。根命令上的 `PersistentPreRunE` / `PersistentPostRunE` 钩子使 pprof 采集无侵入地嵌入每一条子命令，无需重复代码。

3. **Factory `f`（依赖注入）**：`cmdutil.NewFactory()` 封装与 kube-apiserver 通信的客户端（认证、版本协商、REST mapper），以依赖注入方式传给所有子命令——子命令本身不建立连接，只从 `f` 取客户端用。

4. **Builder 建造者模式**：create 命令用 Builder 链式收集配置（schema、namespace、文件路径等），所有方法均返回 `*Builder` 支持链式调用，最终 `Do()` 生成可遍历的 Result。

5. **Info 对象——Visitor 链的数据载体**：`StreamVisitor` 把 yaml 字节解码后生成 `Info`，包含 k8s 对象本体、namespace、REST mapping 和客户端，是发一次 API 请求所需的全部信息。Builder 产出 Info，Visitor 链对其处理，最终 `RESTClient.Post()` 将其发送出去。

6. **Visitor 访问者模式与 DecoratedVisitor**：Result 内部是一组 Visitor 的嵌套组合，每个 Visitor 只负责一件事（加载文件、校验 namespace、过滤集群级资源……）。`DecoratedVisitor` 在业务回调执行前依次运行所有 helper，是装饰器模式与访问者模式的叠加。

---

## 全章调用链总图

> 本图展示从 `kubectl create -f` 到 `kube-apiserver` 的完整调用路径，标注了每步对应的源文件和章节，阅读各节时可随时对照定位当前所处位置。

```
kubectl create -f nginx.yaml
  │
  ▼  kubectl.go — main() → command.Execute()                         §02
  │
  ▼  cmd.go — NewKubectlCommand()                                    §03 / §04
  │    ├─ PersistentPreRunE  → profiling.go initProfiling()          §03
  │    ├─ PersistentPostRunE → profiling.go flushProfiling()         §03
  │    └─ f := cmdutil.NewFactory(...)  ← Factory 依赖注入            §04
  │
  ▼  cobra 路由到 create 子命令
  │
  ▼  create.go — NewCmdCreate() → RunCreate()                        §05
  │
  ├─── --raw    → rawhttp.RawPost()         ← 直接发原始请求
  ├─── --edit   → RunEditOnCreate()         ← 打开编辑器再创建
  └─── 常规路径
         │
         ▼  builder.go — f.NewBuilder().FilenameParam(...).Do()      §06
         │
         │    FilenameParam → b.Path()
         ▼  visitor.go — FileVisitor → StreamVisitor                 §07
         │
         ▼  mapper.go — infoForData()                                §07
         │              → Info{ Object, GVK, Namespace, Client }
         │
         ▼  builder.go — Do() → NewDecoratedVisitor(helpers...)      §07
         │               helpers:
         │               ├─ RequireNamespace   (namespace 校验)
         │               ├─ FilterNamespace    (过滤集群级资源)
         │               └─ RetrieveLazy       (按需拉取远端状态)
         │
         ▼  result.go — r.Visit(outerFn)                             §07
         │
         ▼  create.go — 外层 VisitorFunc                             §07
         │              resource.NewHelper().Create()
         │
         ▼  interfaces.go — RESTClient.Post()                        §05 / §07
         │
         ▼  kube-apiserver
```

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

---

## 02. 命令行解析工具cobra的使用

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| kubectl 程序入口 | [kubectl.go](kubernetes/cmd/kubectl/kubectl.go) | `main()` |
| cobra 根命令构造 | [cmd.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `NewDefaultKubectlCommand():420` |

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

### kubectl 的入口：main()

kubectl 的入口是 `cmd/kubectl/kubectl.go`，整个 main 函数只做三件事：

```go
// cmd/kubectl/kubectl.go
func main() {
    rand.Seed(time.Now().UnixNano())

    command := cmd.NewDefaultKubectlCommand() // 构建整棵 cobra.Command 树

    pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
    pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
    logs.InitLogs()
    defer logs.FlushLogs()

    if err := command.Execute(); err != nil { // cobra 路由 + 执行
        os.Exit(1)
    }
}
```

`NewDefaultKubectlCommand()` 在 `staging/src/k8s.io/kubectl/pkg/cmd/` 中定义，它把所有子命令注册到一棵 `cobra.Command` 树上并返回根节点。`command.Execute()` 由 cobra 接管，负责解析命令行字符串、找到匹配的子命令节点、调用其 `Run/RunE` 函数。

### cobra 的三个核心概念

cobra 是 Go 生态中最主流的 CLI 框架（kubectl、hugo、docker 都用它）。它把一条命令拆分为三个部分：

| 概念 | 含义 | 对应 git clone 示例 |
|------|------|---------------------|
| **Commands** | 执行动作，即子命令 | `clone`（动作） |
| **Args** | 位置参数，跟在命令后面 | `https://github.com/spf13/cobra.git`（仓库地址） |
| **Flags** | 标识符，以 `-` 或 `--` 开头 | `--bare`（创建裸库） |

以 `git clone https://github.com/spf13/cobra.git --bare` 为例：`git` 是可执行文件，`clone` 是 Command，URL 是 Arg，`--bare` 是 Flag。

### cobra.Command 的结构

每个节点是一个 `cobra.Command` 结构体：

```go
var rootCmd = &cobra.Command{
    Use:   "my_cobra",                              // 命令名
    Short: "A brief description of your application",
    Long:  `A longer description...`,
    Run: func(cmd *cobra.Command, args []string) {  // 执行函数
        fmt.Println("my_cobra")
    },
}
```

子命令通过 `rootCmd.AddCommand(subCmd)` 挂载。cobra 路由时，根据用户输入的字符串逐层匹配，找到对应节点后调用其 `Run` 函数：

```
go run main.go container   →  执行 containerCmd.Run，输出 "container called"
go run main.go version     →  执行 versionCmd.Run，输出 "my_cobra version is v1.0"
```

### Flags：persistent 与 local 的区别

flags 按作用范围分两类：

- **persistent**：对当前 Command 及其所有子 Command 生效，适合全局开关（如 `--verbose`）：
  ```go
  var Verbose bool
  rootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "verbose output")
  ```

- **local**：只对当前 Command 生效，不向子 Command 传递：
  ```go
  var Source string
  rootCmd.Flags().StringVarP(&Source, "source", "s", "", "Source directory to read from")
  ```

kubectl 中，`-n`（namespace）等全局标志用 persistent，`-f`（文件）等特定于某个子命令的标志用 local。

### Args：位置参数

以 `ls [OPTION]... [FILE]...` 为例：`OPTION` 对应 flags（以 `-` 开头），`FILE` 对应 arguments（位置参数）。cobra 中称为 Args，规则是参数在所有 flags 之后，`...` 表示可以指定多个。

---

## 03. kubectl命令行设置pprof抓取火焰图

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| 根命令与钩子注册 | [cmd.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `NewKubectlCommand:472` |
| pprof flag 注册 | [profiling.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/profiling.go) | `addProfilingFlags:34` |
| 采集启动 | [profiling.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/profiling.go) | `initProfiling:39` |
| 结果写盘 | [profiling.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/profiling.go) | `flushProfiling:78` |

本节聚焦 `NewKubectlCommand()` 如何利用 cobra 的 `PersistentPreRunE` / `PersistentPostRunE` 钩子，把 pprof 性能采集无侵入地嵌入所有子命令。

### NewKubectlCommand 是真正的根命令构造函数

`NewDefaultKubectlCommand()` 只是一个薄封装，真正的逻辑在：

```
staging/src/k8s.io/kubectl/pkg/cmd/cmd.go
```

```go
// staging/src/k8s.io/kubectl/pkg/cmd/cmd.go:472
func NewKubectlCommand(in io.Reader, out, err io.Writer) *cobra.Command {
    cmds := &cobra.Command{
        Use:   "kubectl",
        // Hook before and after Run initialize and write profiles to disk,
        // respectively.
        PersistentPreRunE: func(*cobra.Command, []string) error {
            rest.SetDefaultWarningHandler(warningHandler)
            return initProfiling()   // 命令执行前：按 --profile 标志启动采集
        },
        PersistentPostRunE: func(*cobra.Command, []string) error {
            if err := flushProfiling(); err != nil { // 命令执行后：将采集结果写盘
                return err
            }
            // ...处理 warnings-as-errors
            return nil
        },
    }

    addProfilingFlags(flags) // 注册 --profile 和 --profile-output 两个 flag
    // ...注册所有子命令
}
```

`PersistentPreRunE` 和 `PersistentPostRunE` 是 cobra 的生命周期钩子，对根命令设置后，所有子命令（create、get、apply 等）执行时都会自动触发，无需在每个子命令里重复写。这就是为什么任意 kubectl 命令都支持 `--profile`。

### pprof 的两个 flag

```go
// staging/src/k8s.io/kubectl/pkg/cmd/profiling.go:34
func addProfilingFlags(flags *pflag.FlagSet) {
    flags.StringVar(&profileName, "profile", "none",
        "Name of profile to capture. One of (none|cpu|heap|goroutine|threadcreate|block|mutex)")
    flags.StringVar(&profileOutput, "profile-output", "profile.pprof",
        "Name of the file to write the profile to")
}
```

- `--profile`：指定采集哪种指标，默认 `none`（不采集）
- `--profile-output`：结果写入哪个文件，默认 `profile.pprof`

`initProfiling` 根据 `--profile` 值按类型启动采集（CPU 流式写入需开始/停止配对，block/mutex 需先设采样率，其余类型在结束时一次性写快照）；同时注册 Ctrl-C 信号处理，确保中途打断也能落盘。`flushProfiling` 负责停止采集并将结果写入 `--profile-output` 指定的文件。

### 实际使用：采集 kubectl get node 的 CPU 火焰图

```bash
# 采集 CPU profile
kubectl get node --profile=cpu --profile-output=cpu.pprof

# 生成 SVG 火焰图（可在浏览器打开，清晰展示调用链和耗时）
go tool pprof -svg cpu.pprof > cpu.svg

# 或以文本形式查看 goroutine 快照
kubectl get node --profile=goroutine --profile-output=goroutine.pprof
go tool pprof --text goroutine.pprof
```

火焰图中可以看到完整调用链：`main` → `cobra.(*Command).Execute` → 各子命令函数，以及每一层的 CPU 占用时间。这是分析 kubectl 性能瓶颈、理解调用路径的利器。



---

## 04. kubectl命令行设置7大命令分组

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| Factory 创建 | [cmd.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `cmdutil.NewFactory:531` |
| 7 大分组定义 | [cmd.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/cmd.go) | `templates.CommandGroups:551` |

### NewKubectlCommand 的整体结构

`NewKubectlCommand` 除了设置 pprof 钩子，还要完成两件事：创建与 kube-apiserver 通信的工厂对象 `f`，然后用 `f` 构建 7 大命令分组并挂载到根 cobra.Command 上。

```
NewKubectlCommand()
  │
  ├── 创建 rootCmd (cobra.Command)
  │     ├── PersistentPreRunE  → initProfiling()
  │     └── PersistentPostRunE → flushProfiling()
  │
  ├── 设置全局 flags（kubeconfig、namespace、--profile 等）
  │
  ├── f := cmdutil.NewFactory(...)   ← 封装 kube-apiserver 客户端
  │
  └── groups.Add(cmds)              ← 7大分组全部挂载到根命令
        ├── Basic Commands (Beginner)
        ├── Basic Commands (Intermediate)
        ├── Deploy Commands
        ├── Cluster Management Commands
        ├── Troubleshooting and Debugging Commands
        ├── Advanced Commands
        └── Settings Commands
```

### cmd 工厂函数 f 的作用

```go
// staging/src/k8s.io/kubectl/pkg/cmd/cmd.go:531
f := cmdutil.NewFactory(matchVersionKubeConfigFlags)
```

`f` 是后续所有子命令共享的工厂对象，封装了与 kube-apiserver 交互所需的客户端（认证、版本协商、REST mapper 等）。每个子命令的构造函数都接收 `f` 作为参数，通过它发起 API 请求。这是依赖注入的典型用法——子命令本身不负责建立连接，只从 `f` 取客户端用。

### 7 大分组的定义

| 分组 | 代表命令 |
|------|---------|
| Basic Commands (Beginner) | `create`、`expose`、`run`、`set` |
| Basic Commands (Intermediate) | `explain`、`get`、`edit`、`delete` |
| Deploy Commands | `rollout`、`scale`、`autoscale` |
| Cluster Management Commands | `top`、`cordon`、`drain`、`taint` |
| Troubleshooting and Debugging | `describe`、`logs`、`exec`、`port-forward` |
| Advanced Commands | `diff`、`apply`、`patch`、`replace` |
| Settings Commands | `label`、`annotate`、`completion` |

每个命令构造函数均接收工厂 `f` 和 `ioStreams` 作为参数，返回独立的 `*cobra.Command`。完整列表见 [cmd.go:551](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/cmd.go)。

### 分组的设计意图

每个分组的 `Message` 字段就是 `kubectl --help` 时看到的标题。`groups.Add(cmds)` 把这 7 组命令统一注册为根命令的子命令，并按分组格式化帮助输出。

每个子命令（如 `create.NewCmdCreate`）接收工厂 `f` 和 `ioStreams`，返回一个独立的 `*cobra.Command`——这意味着添加新子命令只需实现一个返回 `*cobra.Command` 的函数，再加一行 `Groups` 定义即可，扩展成本极低。



---

## 05. create命令执行流程

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| create 子命令注册 | [create.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/create/create.go) | `NewCmdCreate:100` |
| create 核心执行逻辑 | [create.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/create/create.go) | `RunCreate:224` |
| 文件来源抽象 | [builder.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/builder.go) | `FilenameParam:236` |
| REST 通信接口 | [interfaces.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/interfaces.go) | `RESTClient:39` |

### 执行流程概览

```
kubectl create -f nginx.yaml
  │
  ▼ cobra 路由到 NewCmdCreate.Run
  │
  ▼ o.Complete(f, cmd)       — 完善选项（printer、recorder 等）
  │
  ▼ o.ValidateArgs(cmd, args) — 校验参数互斥关系
  │
  ▼ o.RunCreate(f, cmd)
        │
        ├─── 有 --raw？ → rawhttp.RawPost() 直接发送原始请求
        │
        ├─── 有 --edit？ → RunEditOnCreate() 打开编辑器后创建
        │
        └─── 常规路径：
              │
              ▼ f.NewBuilder().FilenameParam(...).Do()  — 构建 resourceBuilder
              │
              ▼ r.Visit(func(info) { ... })            — 遍历每个资源
                    │
                    └── resource.NewHelper(...).Create() → RESTClient.Post()
                                                                │
                                                                ▼
                                                          kube-apiserver
```

### NewCmdCreate：注册子命令与 Run 逻辑

```go
// staging/src/k8s.io/kubectl/pkg/cmd/create/create.go:100
func NewCmdCreate(f cmdutil.Factory, ioStreams genericclioptions.IOStreams) *cobra.Command {
    o := NewCreateOptions(ioStreams)

    cmd := &cobra.Command{
        Use:   "create -f FILENAME",
        Short: i18n.T("Create a resource from a file or from stdin."),
        Run: func(cmd *cobra.Command, args []string) {
            if cmdutil.IsFilenameSliceEmpty(o.FilenameOptions.Filenames, o.FilenameOptions.Kustomize) {
                ioStreams.ErrOut.Write([]byte("Error: must specify one of -f and -k\n\n"))
                defaultRunFunc := cmdutil.DefaultSubCommandRun(ioStreams.ErrOut)
                defaultRunFunc(cmd, args)
                return
            }
            cmdutil.CheckErr(o.Complete(f, cmd))
            cmdutil.CheckErr(o.ValidateArgs(cmd, args))
            cmdutil.CheckErr(o.RunCreate(f, cmd))
        },
    }

    // 绑定 flags 到 o 的各个字段
    cmdutil.AddFilenameOptionFlags(cmd, &o.FilenameOptions, "to use to create the resource")
    cmd.Flags().BoolVar(&o.EditBeforeCreate, "edit", o.EditBeforeCreate, "Edit the API resource before creating")
    cmd.Flags().StringVar(&o.Raw, "raw", o.Raw, "Raw URI to POST to the server.")
    // ...

    // create 本身也有子命令（指定资源类型快捷创建）
    cmd.AddCommand(NewCmdCreateNamespace(f, ioStreams))
    cmd.AddCommand(NewCmdCreateSecret(f, ioStreams))
    cmd.AddCommand(NewCmdCreateDeployment(f, ioStreams))
    cmd.AddCommand(NewCmdCreateJob(f, ioStreams))
    // ... 共 16 个子命令
    return cmd
}
```

`create` 本身支持 `-f` 通用创建，同时又有 `create deployment`、`create job` 等子命令提供更方便的快捷创建路径。

### RunCreate：核心执行逻辑

```go
// staging/src/k8s.io/kubectl/pkg/cmd/create/create.go:224
func (o *CreateOptions) RunCreate(f cmdutil.Factory, cmd *cobra.Command) error {
    // 特殊路径 1：--raw 直接 POST 原始 URI
    if len(o.Raw) > 0 {
        restClient, err := f.RESTClient()
        if err != nil { return err }
        return rawhttp.RawPost(restClient, o.IOStreams, o.Raw, o.FilenameOptions.Filenames[0])
    }

    // 特殊路径 2：--edit 先打开编辑器
    if o.EditBeforeCreate {
        return RunEditOnCreate(f, o.PrintFlags, o.RecordFlags, o.IOStreams, cmd, &o.FilenameOptions, o.fieldManager)
    }

    // 常规路径：--validate 开启 schema 校验（避免 yaml 配置错误）
    schema, err := f.Validator(cmdutil.GetFlagBool(cmd, "validate"))
    if err != nil { return err }

    // 从 kubeconfig 获取 namespace
    cmdNamespace, enforceNamespace, err := f.ToRawKubeConfigLoader().Namespace()
    if err != nil { return err }

    // 构建 Builder，链式配置所有参数，Do() 触发文件读取
    r := f.NewBuilder().
        Unstructured().
        Schema(schema).
        ContinueOnError().
        NamespaceParam(cmdNamespace).DefaultNamespace().
        FilenameParam(enforceNamespace, &o.FilenameOptions). // 读取 -f 指定的文件
        LabelSelectorParam(o.Selector).
        Flatten().
        Do()
    if err = r.Err(); err != nil { return err }

    // 遍历每个资源对象，逐一发送创建请求
    err = r.Visit(func(info *resource.Info, err error) error {
        if err != nil { return err }
        // ... 注解处理、dry-run 处理
        obj, err := resource.NewHelper(info.Client, info.Mapping).
            Create(info.Namespace, true, info.Object) // 发送 POST 请求
        if err != nil { return cmdutil.AddSourceToErr("creating", info.Source, err) }
        info.Refresh(obj, true)
        return o.PrintObj(info.Object) // 打印创建结果
    })
    return err
}
```

### FilenameParam：文件来源的抽象

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go:236
func (b *Builder) FilenameParam(enforceNamespace bool, filenameOptions *FilenameOptions) *Builder {
    paths := filenameOptions.Filenames
    for _, s := range paths {
        switch {
        case s == "-":
            b.Stdin()                        // 从标准输入读取
        case strings.Index(s, "http://") == 0 || strings.Index(s, "https://") == 0:
            url, _ := url.Parse(s)
            b.URL(defaultHttpGetAttempts, url) // 从 HTTP/HTTPS URL 读取
        default:
            b.Path(recursive, s)              // 从本地文件或目录读取
        }
    }
    return b
}
```

三种来源（stdin、URL、本地文件）在这里统一抽象为 Visitor，后续 `r.Visit()` 无需关心来源差异。

### 底层通信：RESTClient 接口

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/interfaces.go:39
type RESTClient interface {
    Get()    *rest.Request
    Post()   *rest.Request
    Patch(types.PatchType) *rest.Request
    Delete() *rest.Request
    Put()    *rest.Request
}
```

`resource.NewHelper(...).Create()` 最终调用 `RESTClient.Post()`，构建 REST 请求发送给 kube-apiserver。`RESTClient` 是接口而非具体实现，方便测试时替换为 fake client。


---

## 06. createCmd中的builder建造者设计模式

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| Builder 结构体定义 | [builder.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/builder.go) | `Builder struct:52` |
| Builder 构造函数 | [builder.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/builder.go) | `NewBuilder:201` |
| 链式方法示例 | [builder.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/builder.go) | `Schema:217`, `ContinueOnError:222` |

### 建造者模式的定义

建造者（Builder）模式：将一个**复杂对象的构造与它的表示分离**，使同样的构建过程可以创建不同的对象。核心思路是把复杂对象分解为多个简单的部分，然后一步步组装。

**优点**：
- 构建和表示分离，客户端不需要知道产品内部细节
- 各个具体建造者相互独立，有利于解耦
- 可以对创建过程逐步细化，不影响其他模块

**缺点**：
- 产品的组成结构必须相同，限制了使用范围
- 产品内部变化复杂时，建造者也要同步修改，维护成本较大

### kubectl 中的 Builder 对象

`f.NewBuilder()` 返回的是 `staging/src/k8s.io/cli-runtime/pkg/resource/builder.go` 中的 `Builder` 结构体。kubectl 用它来体现建造者模式有三个典型特征。

**特点 1：针对复杂对象的创建，字段非常多**

`Builder` 的字段涵盖了所有可能的配置维度，直接用构造函数传参会导致几十个参数：

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go:52
type Builder struct {
    categoryExpanderFn CategoryExpanderFunc
    mapper             *mapper          // 资源类型映射
    clientConfigFn     ClientConfigFunc // 生成客户端的函数
    restMapperFn       RESTMapperFunc
    objectTyper        runtime.ObjectTyper       // 对象类型识别
    negotiatedSerializer runtime.NegotiatedSerializer // 序列化方式
    local              bool             // 是否禁止 server 调用
    errs               []error
    paths              []Visitor        // 文件/URL/stdin 来源
    stream             bool
    labelSelector      *string
    fieldSelector      *string
    resources          []string
    namespace          string
    allNamespace       bool
    names              []string
    flatten            bool
    continueOnError    bool
    singleItemImplied  bool
    schema             ContentValidator // schema 校验器
    // ...共约 20 个字段
}
```

如果用 `NewBuilder(field1, field2, field3, ...)` 传所有参数，调用方会极难使用，且大部分参数在每次调用时并不需要。建造者模式正好解决这个问题。

**特点 2：入口函数返回要构建对象的指针**

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go:201
func NewBuilder(restClientGetter RESTClientGetter) *Builder {
    // ...初始化必要依赖
    return newBuilder(
        restClientGetter.ToRESTConfig,
        (&cachingRESTMapperFunc{...}).ToRESTMapper,
        (&cachingCategoryExpanderFunc{...}).ToCategoryExpander,
    )
}
```

`NewBuilder` 只负责初始化最基础的依赖，返回一个空白的 `*Builder`，其余字段由后续链式调用按需设置。

**特点 3：所有方法都返回建造对象本身的指针（链式调用）**

每个配置方法都只做一件事：设置某个字段，然后 `return b`：

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go:217
func (b *Builder) Schema(schema ContentValidator) *Builder {
    b.schema = schema
    return b // 返回 *Builder 自身
}

func (b *Builder) ContinueOnError() *Builder {
    b.continueOnError = true
    return b
}
```

这使得调用方可以写成流畅的链式风格，每一步都在描述"我需要什么"，而不是一次性传入所有参数：

```go
// staging/src/k8s.io/kubectl/pkg/cmd/create/create.go
r := f.NewBuilder().
    Unstructured().                                     // 使用非结构化解码（map 格式）
    Schema(schema).                                     // 设置 schema 校验器
    ContinueOnError().                                  // 遇到错误继续而不中断
    NamespaceParam(cmdNamespace).DefaultNamespace().    // 设置 namespace
    FilenameParam(enforceNamespace, &o.FilenameOptions). // 设置文件来源
    LabelSelectorParam(o.Selector).                     // 标签过滤
    Flatten().                                          // 展平 List 类型
    Do()                                                // 触发构建，返回 Result
```

`Unstructured()` 会设置 `b.objectTyper` 和 `b.mapper`，告诉 Builder 使用 JSON map 格式（而非 Go 类型）处理资源对象，保留所有字段不丢失数据。`Do()` 是终止方法，它读取所有配置，加载文件，返回可遍历的 `Result` 对象。

`Result` 内部持有的正是一组 Visitor 的嵌套结构——这就引出了下一节要分析的 Visitor 模式。


---

## 07. createCmd中的visitor访问者设计模式

| 读码目标 | 源文件 | 入口 |
|---------|--------|------|
| Visitor / VisitorFunc 接口 | [interfaces.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/interfaces.go) | `Visitor:39` |
| Info 对象解码 | [mapper.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/mapper.go) | `infoForData` |
| FileVisitor / StreamVisitor | [visitor.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go) | `FileVisitor.Visit`, `StreamVisitor.Visit` |
| Do() 组装 Visitor 链 | [builder.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/builder.go) | `Do:1116` |
| DecoratedVisitor | [visitor.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go) | `DecoratedVisitor:305` |
| RequireNamespace | [visitor.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go) | `RequireNamespace:625` |
| Result.Visit | [result.go](kubernetes/staging/src/k8s.io/cli-runtime/pkg/resource/result.go) | `Visit:95` |
| 外层 VisitorFunc（真正创建资源） | [create.go](kubernetes/staging/src/k8s.io/kubectl/pkg/cmd/create/create.go) | `RunCreate:224` |

### Visitor 模式的定义

Visitor（访问者）模式将**数据结构**与**对数据结构的操作**分离，把操作封装进独立的 Visitor 对象。好处是：添加新操作时不需要修改数据结构本身，只需新增一个 Visitor。

kubectl 中的使用方式体现了这一点：一份资源数据（`Info` 对象）被多个 Visitor 依次访问，每个 Visitor 只处理一件事（加载文件、校验 schema、设置 namespace……），彼此完全解耦。把数据结构比作数据库，每个 Visitor 就是一个独立的小应用，各自处理自己的逻辑。

**优点**：数据与操作各自独立，易于扩展新操作。
**缺点**：若要新增新的元素类型（数据结构字段），则所有 Visitor 都要同步修改，依赖关系反转。

---

### 核心接口

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/interfaces.go
type Visitor interface {
    Visit(VisitorFunc) error
}

type VisitorFunc func(*Info, error) error
```

- `Visitor` 接口只有一个方法 `Visit`，接收一个 `VisitorFunc` 回调。
- `VisitorFunc` 是函数类型，参数是 `*Info`（当前处理的资源对象）和可能的 `error`，返回 `error`。
- 整个 Visitor 链的驱动都靠这两个类型，简洁且统一。

---

### Info 对象——k8s 资源的载体

`infoForData` 函数（`staging/src/k8s.io/cli-runtime/pkg/resource/mapper.go`）把原始字节解码为 `Info`：

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/mapper.go
func (m *mapper) infoForData(data []byte, source string) (*Info, error) {
    obj, gvk, err := m.decoder.Decode(data, nil, nil) // 解析出 k8s 对象和 GVK
    if err != nil {
        return nil, fmt.Errorf("unable to decode %q: %v", source, err)
    }

    name, _      := metadataAccessor.Name(obj)
    namespace, _ := metadataAccessor.Namespace(obj)
    resourceVersion, _ := metadataAccessor.ResourceVersion(obj)

    ret := &Info{
        Source:          source,
        Namespace:       namespace,
        Name:            name,
        ResourceVersion: resourceVersion,
        Object:          obj,  // 这就是 k8s 对象本体
        ...
    }
    ...
}
```

- `m.decoder.Decode` 把 yaml/json 字节解析成 `runtime.Object`（所有 k8s 类型的基类接口）和 GVK（Group/Version/Kind）。
- `metadataAccessor` 从对象中提取 Name、Namespace、ResourceVersion 等元数据。
- `restMapper.RESTMapping(gvk.GroupKind, gvk.Version)` 把 GVK 映射到 REST 端点（决定请求发到哪个 URL）。
- 最终 `Info` 包含了发一次 API 请求所需的全部信息：资源对象、namespace、客户端、REST mapping。

---

### FileVisitor 与 StreamVisitor

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go
func (v *FileVisitor) Visit(fn VisitorFunc) error {
    // 打开文件（或从 stdin 读取），包装成 StreamVisitor，委托给它
    ...
    return v.StreamVisitor.Visit(fn)
}

func (v *StreamVisitor) Visit(fn VisitorFunc) error {
    d := yaml.NewYAMLOrJSONDecoder(v.Reader, 4096)
    for {
        ext := runtime.RawExtension{}
        if err := d.Decode(&ext); err != nil { ... }
        ...
        info, err := v.infoForData(ext.Raw, v.Source) // 转换为 Info 对象
        if err != nil { ... }
        if err := fn(info, nil); err != nil { return err }
    }
}
```

- `FileVisitor` 负责打开文件（支持 stdin、本地路径），然后把工作交给 `StreamVisitor`。
- `StreamVisitor` 用 `yaml.NewYAMLOrJSONDecoder` 循环解码，一份 yaml 文件可包含多个文档（以 `---` 分隔），每个文档生成一个 `Info`，依次调用 `fn`。

---

### FilenameParam 与 b.Path()

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go
func (b *Builder) FilenameParam(enforceNamespace bool, filenameOptions *FilenameOptions) *Builder {
    ...
    // validate 检查 -f 与 -k 不能同时使用
    for _, s := range filenameOptions.Filenames {
        switch {
        case s == "-":       b.Stdin()
        case strings.Index(s, "://") != -1: b.URL(...)
        default:             b.Path(recursive, s)  // 本地文件走这里
        }
    }
    ...
}
```

`b.Path()` 最终调用 `ExpandPathsToFileVisitors`，使用 `filepath.Walk` 递归遍历目录，为每个匹配的文件创建一个 `FileVisitor`，追加到 `b.paths []Visitor` 中。

---

### Do() 组装 Visitor 链

`Builder.Do()` 是整个构建阶段的终点，它把所有收集好的 Visitor 用 `DecoratedVisitor` 包装成一条完整的执行链：

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/builder.go:1116
func (b *Builder) Do() *Result {
    r := b.visitorResult()
    r.mapper = b.Mapper()
    if r.err != nil {
        return r
    }
    if b.flatten {
        r.visitor = NewFlattenListVisitor(r.visitor, b.objectTyper, b.mapper)
    }
    helpers := []VisitorFunc{}
    if b.defaultNamespace {
        helpers = append(helpers, SetNamespace(b.namespace))
    }
    if b.requireNamespace {
        helpers = append(helpers, RequireNamespace(b.namespace))   // 校验 namespace
    }
    helpers = append(helpers, FilterNamespace)
    if b.requireObject {
        helpers = append(helpers, RetrieveLazy)
    }
    if b.continueOnError {
        r.visitor = NewDecoratedVisitor(ContinueOnErrorVisitor{r.visitor}, helpers...)
    } else {
        r.visitor = NewDecoratedVisitor(r.visitor, helpers...)     // 包装为 DecoratedVisitor
    }
    return r
}
```

`helpers` 是一批 `VisitorFunc`，每个处理一个关注点：
- `SetNamespace`：对没有指定 namespace 的资源自动填充
- `RequireNamespace`：校验资源的 namespace 与命令行参数是否一致，不一致则报错
- `FilterNamespace`：过滤掉集群级资源（不属于任何 namespace 的资源）
- `RetrieveLazy`：按需从 API Server 拉取最新资源状态

---

### DecoratedVisitor——装饰器模式的叠加

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go:305
type DecoratedVisitor struct {
    visitor    Visitor
    decorators []VisitorFunc
}

func NewDecoratedVisitor(v Visitor, fn ...VisitorFunc) Visitor {
    if len(fn) == 0 {
        return v
    }
    return DecoratedVisitor{v, fn}
}

// Visit implements Visitor
func (v DecoratedVisitor) Visit(fn VisitorFunc) error {
    return v.visitor.Visit(func(info *Info, err error) error {
        if err != nil {
            return err
        }
        for i := range v.decorators {
            if err := v.decorators[i](info, nil); err != nil { // 依次执行每个 helper
                return err
            }
        }
        return fn(info, nil) // 最后执行外层传入的 fn
    })
}
```

`DecoratedVisitor` 是装饰器模式的实现：它包裹一个内层 Visitor，在调用外层 `fn` 之前，先依次执行所有 `decorators`（即 helpers）。这样 `helpers` 中的每个 `VisitorFunc` 都能在资源到达业务逻辑之前对其进行预处理或校验。

---

### RequireNamespace 的实现

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/visitor.go:625
func RequireNamespace(namespace string) VisitorFunc {
    return func(info *Info, err error) error {
        if err != nil {
            return err
        }
        if !info.Namespaced() {            // 集群级资源，跳过
            return nil
        }
        if len(info.Namespace) == 0 {      // 资源没有 namespace，自动填充
            info.Namespace = namespace
            UpdateObjectNamespace(info, nil)
            return nil
        }
        if info.Namespace != namespace {   // namespace 冲突，报错
            return fmt.Errorf("the namespace from the provided object %q does not match the namespace %q...", ...)
        }
        return nil
    }
}
```

---

### Visitor 调用链全貌

执行链从 `RunCreate` 中的 `r.Visit(fn)` 触发，层层向内调用：

```
RunCreate 调用 r.Visit(outerFn)
  │
  ▼ Result.Visit (result.go:95)
  │   err := r.visitor.Visit(fn)
  │
  ▼ DecoratedVisitor.Visit
  │   内层 visitor.Visit(包装后的fn)
  │     ├─ 依次执行 decorators[i](info, nil)
  │     │   ├─ RequireNamespace：校验 namespace
  │     │   ├─ FilterNamespace：过滤集群级资源
  │     │   └─ RetrieveLazy：按需拉取远端状态
  │     └─ 调用 fn(info, nil)  ← 外层 VisitorFunc
  │
  ▼ FileVisitor.Visit / StreamVisitor.Visit
      解码 yaml → Info → 触发上层 fn
```

```go
// staging/src/k8s.io/cli-runtime/pkg/resource/result.go:95
func (r *Result) Visit(fn VisitorFunc) error {
    if r.err != nil {
        return r.err
    }
    err := r.visitor.Visit(fn)
    return utilerrors.FilterOut(err, r.ignoreErrors...)
}
```

---

### 外层 VisitorFunc——真正创建资源的地方

`RunCreate` 传给 `r.Visit` 的 `fn` 才是最终的业务逻辑：

```go
// staging/src/k8s.io/kubectl/pkg/cmd/create/create.go
err = r.Visit(func(info *resource.Info, err error) error {
    if err != nil {
        return err
    }
    // 记录注解（用于 kubectl apply 的 diff）
    if err := util.CreateOrUpdateAnnotation(...); err != nil {
        return cmdutil.AddSourceToErr("creating", info.Source, err)
    }
    // 记录操作日志（用于 --dry-run 时显示）
    if err := o.Recorder.Record(info.Object); err != nil {
        klog.V(4).Infof("error recording current command: %v", err)
    }
    // DryRunStrategy 处理
    if o.DryRunStrategy == cmdutil.DryRunClient {
        // client 模式：不发请求，直接打印
        if o.DryRunStrategy == cmdutil.DryRunServer {
            // server 模式：发请求但服务端不持久化
            ...
        }
    }
    // 正常路径：创建资源
    obj, err = resource.NewHelper(info.Client, info.Mapping).
        DryRun(o.DryRunStrategy == cmdutil.DryRunServer).
        WithFieldManager(o.fieldManager).
        Create(info.Namespace, true, info.Object)
    if err != nil {
        return cmdutil.AddSourceToErr("creating", info.Source, err)
    }
    info.Refresh(obj, true)

    count++
    return o.PrintObj(info.Object) // 打印创建结果
})
```

- **DryRunStrategy**：`None`（不试运行）、`Client`（仅本地模拟，不发请求）、`Server`（发请求但服务端不持久化变更）。
- `resource.NewHelper(...).Create(...)` 最终调用 `RESTClient.Post`，把资源发送到 API Server。
- `info.Refresh(obj, true)` 用服务端返回的对象（含 UID、resourceVersion 等）刷新本地 Info。
- `o.PrintObj` 打印创建结果（支持 `-o yaml`、`-o json` 等格式）。




---

## 08. kubectl功能和对象总结

### kubectl 中的核心对象

**RESTClient**（与 k8s-apiserver 通信的 REST 客户端）：所有子命令最终都通过 `RESTClient` 接口（Get / Post / Patch / Delete / Put）与 API Server 交互，上层不感知底层 HTTP 细节。接口定义见 §05。

---

**k8s Object**（Kubernetes 对象）：

Kubernetes 对象是集群状态的持久化记录，描述：
- 哪些容器化应用在运行（以及在哪些节点上）
- 可以被应用使用的资源（存储、网络等）
- 应用的运行策略（重启策略、升级策略、容错策略）

对象一旦创建，Kubernetes 系统就会持续工作以确保对象存在于期望状态（Desired State）。操作对象（创建、修改、删除）必须通过 Kubernetes API。

**Spec 与 Status**：每个对象包含两部分：
- `spec`：用户期望的状态，由用户定义和维护
- `status`：对象的实际当前状态，由 Kubernetes 系统持续更新

Kubernetes 的控制循环就是不断让 `status` 趋近于 `spec`。

**yaml 中的必须字段**：

| 字段 | 含义 |
|------|------|
| `apiVersion` | 创建对象所使用的 Kubernetes API 版本 |
| `kind` | 对象的类别（Pod、Deployment 等） |
| `metadata` | 标识对象的元数据：name（唯一标识）、UID、namespace |































