# Kernel 不变量

以下规则是实现和代码评审的硬约束。

## 生命周期

1. `Disposed Scope contains no Active Fiber`。
2. `Disposed` Fiber 不得重新进入非终态。
3. 每个 Fiber 恰好属于一个 Scope。
4. 每个 owned task 和 Effect 恰好有一个 Fiber owner。
5. parent Scope 完成前，所有 child Scope 必须收敛。
6. handle drop 不得替代 Runtime-owned async cleanup。

## 激活与注册

1. 激活失败不留下 committed Service、Invocation、Event、Effect 或 task。
2. staged capability 在 commit 前不可见。
3. registry 是可用性 truth，index/cache 不是。
4. stale generation handle 必须失败，不得命中新资源。
5. 执行第三方 handler 时不得持有 registry writer lock。

## HMR

1. reload commit 前失败时，旧 generation 保持唯一有效。
2. reload commit 后，新工作只能解析到新 generation。
3. 同一次 reload 的 Service、Invocation 和 Event 不得出现混合 generation。
4. 旧 generation 最终必须 dispose，不能永久泄漏。

## Shutdown

1. shutdown linearization 后不能创建新工作或资源。
2. shutdown completion 后不存在 Runtime-owned 活跃任务。
3. cancellation 与 join 使用一个全局 grace deadline。
4. cleanup error 可观察，不能覆盖 primary failure。

## Panic 与并发

1. plugin/handler/task panic 不得破坏 Runtime 状态机。
2. lock guard 不跨第三方或不可控 await。
3. 同一状态迁移最多一个 winner。
4. introspection 是只读路径，不能改变 Runtime state。
5. 已取得 `DisposalCompletion` 的 observer 必须跨 GC 保留 immutable completion；
   未被 poll 的 future 不算已注册 observer，回收后的新请求返回 NotFound。

## 验证要求

任何修复或优化必须带可重复测试。竞态测试优先使用 Barrier、Notify、oneshot、test hook 或 mock clock，禁止依赖 sleep 和 scheduler 运气证明正确性。
