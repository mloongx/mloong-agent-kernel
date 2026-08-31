# Cordis Kernel 架构

## 1. 设计目标

Cordis 解决的问题是：如何在一个异步进程中，以可审计、可取消、可重载、可收敛的方式运行插件及其资源。

设计优先级为：

```text
正确性 → 生命周期安全 → 并发安全 → 渐进复杂度 → 微优化
```

## 2. 分层

### `cordis-core`

只包含稳定合同和领域无关类型：

- generation-aware identity；
- Scope/Fiber 生命周期状态；
- Service、Invocation、Event 的 key 和 typed contract；
- plugin、effect 和错误合同。

该 crate 不拥有 Tokio executor，不实现全局 registry，也不决定资源何时运行。

### `cordis-runtime`

实现：

- Scope/Fiber 树及状态机；
- transactional activation；
- dependency reconciliation；
- Service resolution 与缓存；
- Invocation/Event dispatch；
- owned task 和 Effect cleanup；
- HMR generation cutover；
- shutdown barrier；
- snapshot、health 和 PluginHost。

## 3. Ownership

```text
Runtime
└─ root Scope
   ├─ Fiber
   │  ├─ staged/committed registrations
   │  ├─ owned tasks
   │  └─ Effects
   └─ child Scope
      └─ Fiber
```

Owner 关系必须单向。Context 应使用非拥有引用或受控 handle，禁止用强引用环延长 Runtime 生命周期。

Provider、handler、plugin 实现和调用者都不是 cleanup owner。调用者可以请求取消，最终收尾由 Runtime 完成。

## 4. Truth source

| 事实 | 权威来源 | 非权威副本 |
|---|---|---|
| Scope/Fiber 状态 | Runtime state | 日志、snapshot |
| Service 可用性 | Service registry | resolution cache |
| Invocation handler | Invocation registry | dispatch snapshot |
| Event handler | Event registry | dispatch snapshot |
| dependency 满足情况 | registry 实际内容 | dependency index |
| shutdown 状态 | Runtime state | health/日志 |

缓存与索引只能加速查询，不能延长已释放资源的生命周期，也不能成为第二套可写真相。

## 5. 并发模型

- Runtime state mutation 在短临界区内完成。
- 第三方 async 代码在 registry/state writer lock 之外执行。
- dispatch 先捕获 immutable handler snapshot，再调用 handler。
- lock guard 不跨任意不可控 `await`。
- 所有后台工作必须通过 Runtime-owned task 启动。
- panic 在 plugin、handler 和 task 边界被转换为可观察失败。

## 6. 线性化点

| 操作 | 线性化点 |
|---|---|
| Fiber 激活 | staged resources 原子变为可见 |
| HMR | active generation cutover |
| shutdown | Runtime 从 Running 进入 ShuttingDown |
| Scope dispose | disposal 被永久提交并禁止新增 child/work |
| Effect cleanup | exactly-once cleanup ownership 被消费 |

所有并发 race 都应能根据这些点解释 winner。

## 7. 依赖系统

Kernel dependency 只允许：

```text
DependencyKey::Service(ServiceKey)
DependencyKey::Invocation(InvocationKey)
```

安全权限、工具能力和业务标签不属于 Kernel dependency。

索引目标复杂度为 `O(affected_dependents + changed_edges)`。每次 provider 变化只协调受影响 Fiber，不进行无条件全量扫描。

## 8. 扩展边界

外部实现可以通过 `NativePlugin` 或 `PluginHost` 接入。即使实现位于其他进程，真实 Scope/Fiber 生命周期仍由本地 Runtime 拥有；远端只能作为 capability proxy，不能成为本地生命周期 truth source。

Track B 的稳定语义边界以 [Contract Matrix](contracts/CONTRACT_MATRIX.md)、
[Host Boundary](contracts/HOST_BOUNDARY.md) 和
[Conformance Plan](contracts/CONFORMANCE_PLAN.md) 为准。`PluginHost` 定义
artifact 到 lifecycle proxy 的适配边界；仓库现在包含 B2.1B reference
`ProcessHost` transport foundation and B2.1C remote invocation protocol。

Track B2.0 further separates Host kind, HostSession, and session-scoped remote
routes. None is Runtime authority. The local lifecycle proxy is the only bridge:
remote declarations pass through its exact `Context`, while Runtime retains
Scope/Fiber/generation admission, capability publication, HMR cutover, disposal,
and shutdown truth. See [Host state machine](contracts/HOST_STATE_MACHINE.md).

Reference ProcessHost v2 proves external invocation and lifecycle semantics
across a process boundary. It does not transport Service/Event registries and is
not a security sandbox. Codec, framing, process topology, and task layout remain
implementation choices rather than architecture contracts.

B2.1B uses bounded private frames over child stdio, a fixed
reader/writer/supervisor topology, and a non-clone owner transferred into the
Fiber's first cleanup Effect. Stdio, binary tags, and one child per hosted
generation remain replaceable implementation choices. Scope/Fiber/generation
IDs and `Context` never appear as wire authority.

B2.1C maps bounded private invocation declarations into the same staged
`Context::handle_invocation` path as Native plugins. The remote handler is only a
proxy: Runtime retains the local InvocationId, absolute deadline, cancellation
winner, generation lease, HMR selector, and public result taxonomy. The writer
derives relative budget at actual send; Cancel is one-way best effort. Remote
Service/Event, replay, sandboxing, and automatic Host restart remain
outside this layer.
