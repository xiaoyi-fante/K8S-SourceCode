# 第38章 k8s CRD 开发

> **适用版本**: Kubernetes v1.21
> **对应章节**: 第 38 章 — k8s crd 开发
> **源码入口**: 外部项目 `kubebuilder`；CRD 代码位于 `controllers/appservice_controller.go`

---

## 核心机制一览

1. **CRD 解决结构化数据存储，Controller 负责跟踪**：CRD 本身只是在 etcd 里存一份自定义结构的对象，不包含任何行为逻辑。行为由 Controller（Operator）提供，Controller watch CRD 对象变化，驱动下层资源（Deployment、Service 等）收敛到期望状态。

2. **Operator 模式 = CRD + Controller**：Operator 是对 Kubernetes 声明式 API 设计思想的扩展，用户通过自定义 CRD 表达"意图"，Controller 的 Reconcile loop 负责让"现实"趋近"意图"。Prometheus Operator、etcd Operator 都是典型案例。

3. **kubebuilder 只需写两处**：`kubebuilder init` + `kubebuilder create api` 生成全套脚手架（informer、workqueue、RBAC YAML、Dockerfile、Makefile 等）。开发者只需编写：① `api/v1/appservice_types.go`（定义 CRD 结构体）；② `controllers/appservice_controller.go`（Reconcile 调谐逻辑）。

4. **Reconcile 是幂等的**：每次 CRD 对象发生变化都会触发 Reconcile。函数内部必须先 Get CRD 对象，再 Get 关联资源（dep/svc），判断是否存在再决定 Create 或 Update，不能假设状态。

5. **Annotations 用于检测 Spec 变化**：更新场景下，每次 Reconcile 把当前 Spec 序列化为 JSON 存入 `instance.Annotations["spec"]`，下次调用时用 `reflect.DeepEqual` 对比新旧 Annotations["spec"] 字段，来判断是否需要 update Deployment/Service。

---

## 全章调用链总图

```
用户 kubectl apply AppService CRD 对象
  │
  ▼ apiserver 存入 etcd
  │
  ▼ informer 触发事件 → workqueue 入队
  │
  ▼ AppServiceReconciler.Reconcile(ctx, req)  (appservice_controller.go)
  │
  ├── r.Client.Get(AppService)    — 获取 CRD 对象；NotFound → 忽略
  │     instance.DeletionTimestamp != nil → 忽略（正在删除）
  │
  ├── r.Client.Get(Deployment)
  │     NotFound → NewDeployment(instance) → r.Client.Create(dep)
  │     Found    → Annotations spec 对比 → DeepEqual? 跳过 : r.Client.Update(newDep)
  │
  ├── r.Client.Get(Service)
  │     NotFound → NewService(instance) → r.Client.Create(svc)
  │     Found    → oldService.Spec = newService.Spec → r.Client.Update(oldSvc)
  │
  └── 更新 instance.Annotations["spec"] = json(instance.Spec)
        r.Client.Update(instance)
        return reconcile.Result{}, nil
```

---

## §01 CRD 技术介绍与需求分析

### 1. 为什么需要 CRD + Operator

Kubernetes 本身是"声明式 + 控制器模式"：用户声明期望状态，Controller 让现实状态收敛过去。CRD 把这个模式扩展到自定义领域：

- **CRD**（CustomResourceDefinitions）：在 etcd 里注册一种新 API，提供结构化存储能力。
- **Controller**（Operator 模式）：watch 这种新资源，执行调谐逻辑，驱动底层 Kubernetes 资源。

k8s 1.8 之前要扩展 API 必须写 Extension APIServer，1.8 引入 CRD 后大幅简化，完全由工具生成框架。

### 2. Operator 模式架构

```
Kubernetes API Server
  │  (watch / list)
  ▼
Informer ──► DeltaFIFO ──► WorkQueue
                                │
                                ▼  Sync (每个 key 一次)
                           Custom Controller
                                │
                          ┌─────┴──────┐
                          ▼            ▼
                       Lister     Real-world actions
                    (Local Store)  (create/update/delete)
```

informer 从 apiserver 拿到事件后写入 workqueue，controller worker 取出 key，调 Reconcile 比对期望 vs 实际，执行操作。

### 3. 典型 Operator 举例

| Operator | 说明 |
|----------|------|
| etcd operator | 管理 etcd 集群的创建、扩缩容、备份 |
| Prometheus operator | 通过 `ServiceMonitor` CRD 管理监控目标 |
| grafana, alertmanager | 同 Prometheus operator 套件 |

k8s 内置 controller（如 `kube-controller-manager` 中的 DeploymentController）逻辑一致，核心也是 `Reconcile` loop。

### 4. k8s 内置 controller 模式回顾

以 `DeploymentController.syncDeployment` 为例：

```go
// 计算需要创建多少 pod（replicas - 现有数量）
deploymentReplicasToAdd := allowedSize - allReplicas
switch {
case deploymentReplicasToAdd > 0:
    sort.Sort(controller.ReplicaSetsBySizeNewer(allRSs))
    scalingOperation = "up"
case deploymentReplicasToAdd < 0:
    sort.Sort(controller.ReplicaSetsBySizeOlder(allRSs))
    scalingOperation = "down"
}
```

这正是"对比期望与实际，执行操作"的模式实现。

### 5. 本章 CRD 需求：AppService

目标：定义一个 `AppService` CRD，用户只需声明 nginx 的副本数、镜像和端口，Controller 自动创建对应的 Deployment + NodePort Service。

```yaml
apiVersion: webapp.my.domain/v1
kind: AppService
metadata:
  name: nginx-app
  namespace: my-appservice
spec:
  size: 2
  image: nginx:1.8
  ports:
    - port: 80
      targetPort: 80
      nodePort: 10000
```

用户不再需要手写 Deployment + Service，通过 `AppService` 的 Controller 自动完成"一个声明管两个资源"。

---

## §02 使用 kubebuilder 编写 CRD 代码

### 1. 安装 kubebuilder

kubebuilder 只有 Linux 版本：

```bash
wget https://github.com/kubernetes-sigs/kubebuilder/releases/download/v3.1.0/kubebuilder_linux_amd64
chmod +x kubebuilder_linux_amd64
mv kubebuilder_linux_amd64 /usr/local/bin/kubebuilder
```

### 2. 初始化脚手架项目

```bash
mkdir -p appservice
cd appservice
kubebuilder init --domain my.domain --repo my.domain/appservice
```

`--domain` 指定 CRD 的 apiGroup 域名前缀，`--repo` 对应 Go module 路径。

初始化后生成 24 个文件，关键目录：

```
appservice/
├── config/               # kustomize YAML（RBAC、manager、webhook、certmanager）
├── main.go               # 程序入口
├── Makefile              # make docker-build / make deploy 等命令
└── PROJECT               # kubebuilder 项目元数据
```

### 3. 创建 API（CRD + Controller）

```bash
kubebuilder create api --group webapp --version v1 --kind AppService
# Create Resource [y/n]  → y
# Create Controller [y/n] → y
```

这一步新增关键文件：

| 文件 | 作用 |
|------|------|
| `api/v1/appservice_types.go` | 定义 CRD 结构体 |
| `controllers/appservice_controller.go` | Reconcile 调谐逻辑 |
| `config/samples/webapp_v1_appservice.yaml` | 示例 CR 文件 |
| `config/rbac/appservice_*.yaml` | RBAC 权限声明 |

最终目录 37 个文件，13 个目录。

### 4. 自定义 API 类型（appservice_types.go）

编辑 `api/v1/appservice_types.go`，在 `AppServiceSpec` 中添加业务字段：

```go
// api/v1/appservice_types.go
type AppServiceSpec struct {
    // INSERT ADDITIONAL SPEC FIELDS — desired state of cluster
    Size      int32                        `json:"size"`
    Image     string                       `json:"image"`
    Resources corev1.ResourceRequirements  `json:"resources,omitempty"`
    Envs      []corev1.EnvVar              `json:"envs,omitempty"`
    Ports     []corev1.ServicePort         `json:"ports,omitempty"`
}

type AppServiceStatus struct {
    // INSERT ADDITIONAL STATUS FIELD — define observed state of cluster
    appsv1.DeploymentStatus `json:",inline"`
}
```

**设计要点**：`Ports` 直接复用 `corev1.ServicePort`（同时包含 port/targetPort/nodePort），避免重复定义。`AppServiceStatus` 内嵌 `DeploymentStatus` 直接复用 Deployment 的状态字段。

### 5. Reconcile 调谐函数（appservice_controller.go）

#### Step 1 — 获取 CRD 对象

```go
func (r *AppServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    _ = log.FromContext(ctx)
    reqLogger := log.FromContext(ctx)
    reqLogger.Info("Reconciling AppService")

    // 获取 AppService 对象
    instance := &webappv1.AppService{}
    err := r.Client.Get(context.TODO(), req.NamespacedName, instance)
    if err != nil {
        if errors.IsNotFound(err) {
            // 对象已被删除，无需处理
            return reconcile.Result{}, nil
        }
        return reconcile.Result{}, err
    }
    if instance.DeletionTimestamp != nil {
        return reconcile.Result{}, nil  // 正在删除，跳过
    }
```

#### Step 2 — dep 和 svc 不存在时驱动创建

```go
    // 检查 Deployment 是否存在
    deploy := &appsv1.Deployment{}
    if err := r.Client.Get(context.TODO(), req.NamespacedName, deploy); err != nil && errors.IsNotFound(err) {
        // 1. 创建 Deployment
        deploy = NewDeployment(instance)
        if err := r.Client.Create(context.TODO(), deploy); err != nil {
            return reconcile.Result{}, err
        }
        // 2. 创建 Service（假设 dep 不存在则 svc 也不存在）
        service := NewService(instance)
        if err := r.Client.Create(context.TODO(), service); err != nil {
            reqLogger.Error(err, "svc.create.err")
            return reconcile.Result{}, err
        }
    }
```

#### Step 3 — dep 和 svc 存在时检测是否需要更新

通过 `reflect.DeepEqual` 对比 `Annotations["spec"]` 判断 Spec 是否变化：

```go
    // 对比 Annotations 中缓存的 spec 和当前 spec
    oldSpec := webappv1.AppServiceSpec{}
    if err := json.Unmarshal([]byte(instance.Annotations["spec"]), &oldSpec); err == nil {
        if !reflect.DeepEqual(instance.Spec, oldSpec) {
            // spec 变化 → 更新 Deployment
            newDeploy := NewDeployment(instance)
            oldDeploy := &appsv1.Deployment{}
            r.Client.Get(context.TODO(), req.NamespacedName, oldDeploy)
            oldDeploy.Spec = newDeploy.Spec
            r.Client.Update(context.TODO(), oldDeploy)

            // 更新 Service
            newService := NewService(instance)
            oldService := &corev1.Service{}
            r.Client.Get(context.TODO(), req.NamespacedName, oldService)
            oldService.Spec = newService.Spec
            r.Client.Update(context.TODO(), oldService)
        }
    }
```

#### Step 4 — 更新 Annotations 保存本次 Spec 快照

```go
    // 将本次 Spec 序列化存入 Annotations["spec"]，供下次对比
    data, _ := json.Marshal(instance.Spec)
    if instance.Annotations != nil {
        instance.Annotations["spec"] = string(data)
    } else {
        instance.Annotations = map[string]string{"spec": string(data)}
    }
    r.Client.Update(context.TODO(), instance)

    return reconcile.Result{}, nil
}
```

**为什么用 Annotations 而不直接对比 Spec**：`r.Client.Get` 拿到的是 etcd 当前状态，对比新旧 Spec 需要知道"上一次 Reconcile 时的值"。Annotations 作为轻量 KV 存在对象本身上，持久化在 etcd 中，每次 Reconcile 可靠读到上一次的快照。

### 6. NewDeployment — 构造 Deployment 对象

```go
func NewDeployment(app *v1.AppService) *appsv1.Deployment {
    labels := map[string]string{"app": app.Name}
    selector := &metav1.LabelSelector{MatchLabels: labels}
    return &appsv1.Deployment{
        // ...
        Spec: appsv1.DeploymentSpec{
            Replicas: &app.Spec.Size,
            Template: corev1.PodTemplateSpec{
                Spec: corev1.PodSpec{
                    Containers: newContainers(app),
                },
            },
            Selector: selector,
        },
    }
}

func newContainers(app *v1.AppService) []corev1.Container {
    // 将 ServicePort 转为 ContainerPort
    containerPorts := []corev1.ContainerPort{}
    for _, svcPort := range app.Spec.Ports {
        cport := corev1.ContainerPort{}
        cport.ContainerPort = svcPort.TargetPort.IntVal
        containerPorts = append(containerPorts, cport)
    }
    return []corev1.Container{{
        Name:            app.Name,
        Image:           app.Spec.Image,
        Resources:       app.Spec.Resources,
        Ports:           containerPorts,
        ImagePullPolicy: corev1.PullIfNotPresent,
        Env:             app.Spec.Envs,
    }}
}
```

### 7. NewService — 构造 NodePort Service 对象

```go
func NewService(app *v1.AppService) *corev1.Service {
    return &corev1.Service{
        TypeMeta: metav1.TypeMeta{Kind: "Service", APIVersion: "v1"},
        ObjectMeta: metav1.ObjectMeta{
            Name:      app.Name,
            Namespace: app.Namespace,
            OwnerReferences: []metav1.OwnerReference{
                *metav1.NewControllerRef(app, schema.GroupVersionKind{
                    Group:   v1.GroupVersion.Group,
                    Version: v1.GroupVersion.Version,
                    Kind:    "AppService",
                }),
            },
        },
        Spec: corev1.ServiceSpec{
            Type:  corev1.ServiceTypeNodePort,
            Ports: app.Spec.Ports,
            Selector: map[string]string{"app": app.Name},
        },
    }
}
```

**OwnerReference 的作用**：将 Service 的 ownerReference 设置为对应的 AppService 对象。这样删除 AppService 时，Kubernetes GC 会自动级联删除 Service（不需要在 Reconcile 里手动处理删除逻辑）。

### 8. 执行 go mod tidy

代码写完后执行：

```bash
go mod tidy
```

整理 go.mod / go.sum 依赖，确保编译通过。

---

## §03 部署 CRD 到 k8s 中使用

### 1. 编译打镜像

项目自带 Dockerfile（多阶段构建）：

```dockerfile
# Stage 1: 编译 Go 二进制
FROM golang:1.16 as builder
WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
ENV GOPROXY=https://goproxy.cn   # 解决国内网络问题
RUN go mod download
COPY main.go main.go
COPY api/ api/
COPY controllers/ controllers/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager main.go

# Stage 2: 最小化运行镜像（busybox 替换 gcr.io/distroless）
FROM busybox
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532
ENTRYPOINT ["/manager"]
```

```bash
docker build -t appservice:v1.0 .
docker save appservice:v1.0 | docker load   # 分发到各 node
```

> **注意**：`gcr.io/distroless/static` 镜像在国内拉不到，改用 `busybox` 替代。

### 2. 部署到集群

```bash
# 修改 config/default/manager_auth_proxy_patch.yaml 中的镜像为本地 image
# image: gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0 → 改为可拉取的地址

# 应用所有资源（CRD + controller Deployment）
kubectl apply -f config/samples/webapp_v1_appservice.yaml

# 或通过 make 一键部署
make deploy IMG=appservice:v1.0
```

`make deploy` 的输出（会创建多种资源）：

```
namespace/appservice-system created
customresourcedefinition.apiextensions.k8s.io/appservices.webapp.my.domain configured
serviceaccount/appservice-controller-manager created
clusterrole.rbac.authorization.k8s.io/appservice-manager-role configured
clusterrolebinding.rbac.authorization.k8s.io/appservice-manager-rolebinding configured
deployment.apps/appservice-controller-manager created
```

### 3. RBAC 权限修复

部署后 controller 日志报权限错误，因为生成的 ClusterRole 只有 `appservices` 资源的 watch 权限，缺少 `deployments` 和 `services` 的 create/update 权限。

查看生成的 ClusterRole：

```yaml
# 生成的 appservice-manager-role 只有如下规则
rules:
  - apiGroups: ["webapp.my.domain"]
    resources: ["appservices", "appservices/finalizers", "appservices/status"]
    verbs: [create, delete, get, list, patch, update, watch]
```

需要额外添加：

```yaml
- apiGroups: [""]
  resources: ["services"]
  verbs: ["*"]
- apiGroups: ["apps"]
  resources: ["deployments"]
  verbs: ["*"]
```

```bash
kubectl edit clusterrole appservice-manager-role
# 或 kubectl apply -f 修改后的 yaml
```

### 4. 使用 AppService：创建 nginx 应用

```yaml
# myapp01.yaml
apiVersion: webapp.my.domain/v1
kind: AppService
metadata:
  name: nginx-app
  namespace: my-appservice
spec:
  size: 2
  image: nginx:1.8
  ports:
    - port: 80
      targetPort: 80
      nodePort: 10000
```

```bash
kubectl create ns my-appservice
kubectl apply -f myapp01.yaml
```

控制器自动创建 Deployment（2 副本）和 NodePort Service（10000 端口）：

```bash
kubectl get deployment -n my-appservice
# NAME        READY   UP-TO-DATE   AVAILABLE   AGE
# nginx-app   2/2     2            2           3m

kubectl get svc -n my-appservice
# NAME        TYPE       CLUSTER-IP      PORT(S)         AGE
# nginx-app   NodePort   10.96.186.132   80:10000/TCP    3m

curl localhost:10000   # → nginx 欢迎页
```

验证 api-resources：

```bash
kubectl api-resources | grep appservice
# appservices    webapp.my.domain/v1    true    AppService
```

### 5. 变更 AppService（Reconcile 更新路径）

将 nginx 镜像改为 1.7.9：

```yaml
spec:
  size: 2
  image: nginx:1.7.9
  ports:
    - port: 80
      targetPort: 80
      nodePort: 10000
```

```bash
kubectl apply -f myapp01.yaml
```

观察控制器日志，触发 Reconcile → 检测到 Annotations["spec"] 变化 → 更新 Deployment，旧 Pod 逐步替换为 nginx:1.7.9。

`kubectl describe appservice nginx-app -n my-appservice` 中可以看到 Annotations 的 spec 字段同步更新为新的 JSON。

再改 nodePort 为 10001 → 控制器同步更新 Service，`curl localhost:10001` 访问成功。

### 6. 删除 AppService

```bash
kubectl delete -f myapp01.yaml
# appservice.webapp.my.domain "nginx-app" deleted

kubectl get all -n my-appservice
# No resources found in my-appservice namespace.
```

关联的 Deployment 和 Service 通过 OwnerReference 自动级联删除，无需手动清理。

### 7. 遇到 Terminating namespace 清理方法

删除 namespace 时若卡在 Terminating 状态，通过以下方式强制清理：

```bash
kubectl get ns appservice-system -o json | jq '.spec.finalizers=[]' > ns-without-finalizers.json
cat ns-without-finalizers.json
kubectl proxy &
PID=$!
curl -X PUT http://127.0.0.1:8001/api/v1/namespaces/appservice-system/finalize \
  -H "Content-Type: application/json" \
  --data-binary @ns-without-finalizers.json
kill $PID
```

原理：通过 kubectl proxy 代理本地 8001 端口，直接调 apiserver finalize 接口清空 finalizers，让 namespace 退出 Terminating。
