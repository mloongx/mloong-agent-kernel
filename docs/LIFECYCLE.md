# 生命周期与并发语义

## Scope

Scope 是生命周期和 Service resolution 边界，并形成严格树结构。子 Scope 必须在父 Scope 完成释放前收敛。

Scope 开始 dispose 后：

- 拒绝新 child Scope；
- 拒绝新 Fiber 激活；
- 取消并等待其 Fiber；
- 完成前不得遗留 Active Fiber。

## Fiber 状态机

```text
Created
├─> WaitingDependencies
├─> Starting
└─> Disposing

WaitingDependencies -> Starting | Disposing
Starting -> Active | Failed | Disposing
Active -> Reloading | Failed | Disposing
Reloading -> Active | Failed | Disposing
Failed -> Disposing
Disposing -> Disposed
```

重启语义允许 cleanup 后重新进入依赖等待，但永久 `Disposed` 没有出边。状态转换必须通过 Runtime 的集中入口完成。

## 激活事务

```text
prepare
→ plugin.start
→ 暂存 Service/Invocation/Event/Effect/task
→ 验证依赖和 provides
→ 原子可见性提交
→ Fiber Active
→ 协调受影响 dependent
```

失败路径必须 rollback 所有暂存资源，并同时保留 primary error 与 cleanup error。commit 前其他 Fiber 不得观察到暂存能力。

## Owned task

通过 Context 启动的任务必须具有：

- 唯一 Fiber owner；
- cancellation token；
- completion/panic observation；
- dispose 时的 join；
- 全局 grace deadline。

取消采用广播后并发 join，禁止按任务串行等待 `N × grace`。

## Effect

Effect 是生命周期清理 primitive：

- 只属于一个 Fiber；
- cleanup exactly once；
- shutdown barrier 后不能注册；
- cleanup failure 必须进入 health/error observation。

Effect 不代表持久外部副作用。需要 crash-safe 外部动作时，应由上层 durable action 机制负责。

## HMR

```text
旧 generation Active
→ 创建 staging Scope/Fiber
→ 启动并验证新 revision
→ 原子 generation cutover
→ 旧 generation 对新工作不可见
→ dispose 旧 generation
```

如果 cutover 前失败，旧 generation 继续 Active。Service、Invocation 和 Event 的 active generation 必须在同一个逻辑提交点切换。

## Shutdown

```text
Running -> ShuttingDown -> Complete | Incomplete
```

进入 `ShuttingDown` 后禁止：

- 新 Fiber activation；
- 新 child Scope；
- 新 owned task；
- 新 Service；
- 新 handler；
- 新 Effect。

Shutdown 使用一个共享绝对 deadline。每个 attempt 发布一次 immutable `ShutdownOutcome`；
`Incomplete` 携带仍然存活的 ownership blocker。Runtime admission 不会重新打开，blocker
释放后下一次 `shutdown_detailed` 会创建新的 convergence attempt。`Complete` 发布前，Runtime
已完成 worker/task/generation/staging audit 和最终 GC，此后没有能够修改 Kernel truth 的后台工作。

Automatic GC registration uses the same admission linearization fence as shutdown. A GC request
that wins first is registered before releasing admission and must be drained by shutdown. Once
shutdown closes admission, later finalizers cannot change `gc_state` or create `GcReconcile` workers;
shutdown retains authority to call `collect_garbage` synchronously.

Service symbol capacity is admitted during install/reload lifecycle preparation. Context service
publication validates that the key is declared, then performs lookup-only reuse of that admitted
symbol. Undeclared publication never mutates the monotonic interner.
