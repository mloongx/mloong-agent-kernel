# API 与扩展指南

## 两类 Context

普通消费路径应使用受限 RuntimeContext，只暴露调用、事件、取消和 identity 等能力。

Plugin 激活路径使用 PluginContext/Context，才允许：

- provide Service；
- register Invocation/Event handler；
- register Effect；
- spawn owned task；
- create child Scope。

这种 capability separation 防止普通调用路径意外修改 Runtime 拓扑。

## Service

Service 使用稳定逻辑 key：

```text
ServiceKey(namespace, name, version)
```

Native Rust API 使用 typed `Service<T>`。解析从当前 Scope 向父级查找最近 provider；缓存只是优化，不能持有 disposed provider。

## Invocation

Invocation 使用：

```text
InvocationKey(namespace, name, version)
```

Native 路径保持请求/响应 compile-time typed。跨 host 时使用不可变 bytes envelope，不能传递 Rust 对象地址或 Fiber/Scope handle。

Invocation dispatch 捕获 immutable handler snapshot；middleware 和 handler 在 Runtime writer lock 外执行。

## Event

Event 是短暂、进程内的事实通知，支持 Emit、Bail、Serial、Parallel 和 Waterfall 等 dispatch 行为。Event 不是 durable journal，不能作为业务状态或 crash recovery 的真相。

## Plugin

插件负责声明依赖和在 `start` 阶段注册能力，但不拥有 Runtime 生命周期。插件应：

- 把所有后台工作交给 Context；
- 用 Effect 注册 cleanup；
- 不保存可绕过 generation 检查的内部引用；
- 对取消及时响应；
- 不在 panic 后假设资源仍有效。

## Runtime 配置默认值

```text
task_grace                    2s
shutdown_grace               10s
default_invocation_timeout   60s
max_concurrent_invocations   64
max_scopes                   4096
max_fibers                   8192
max_tasks_per_fiber          128
max_handlers_per_fiber       256
max_effects_per_fiber        256
max_child_scopes_per_fiber   128
max_scope_depth              32
```

所有资源上限和 timeout/grace 必须大于零。

## 错误处理

- 不使用 panic 表达普通运行时错误。
- primary error 与 cleanup error 分开保留。
- timeout、cancel、stale generation、shutdown rejection 必须可区分。
- PluginHost 的 transport error 不得直接改写为成功或普通业务错误。

## Disposal observers

调用 `dispose_scope` 只会在返回的 async future 首次被 poll、成功取得 Scope 内部
`DisposalCompletion` 时注册 observer。仅构造 future 不代表参加了 disposal operation。
已注册 observer 持有 completion Arc，即使 automatic GC 随后回收 Scope，仍必须得到
同一个 immutable completion。Scope 已回收后才首次 poll 的 brand-new request 不属于
旧 operation，返回 `ScopeNotFound`；Runtime 不维护 tombstone history。
