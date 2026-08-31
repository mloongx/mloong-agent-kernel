# mloong-agent-kernel

`mloong-agent-kernel` 是一个与 Agent、LLM 和具体业务无关的 Rust/Tokio 生命周期与插件运行时。它提供 Scope、Fiber、Service、Invocation、Event、Effect、依赖协调、结构化并发、热重载和关闭收敛机制。

当前正式版本为 **v1**，Kernel design contract 已冻结。MSRV 是 Rust 1.85，CI 同时验证 stable，并以 Linux、Windows 为支持平台。

本目录是可独立构建、测试和复用的 Kernel workspace，不依赖上层 `agent-*` crate。

## Workspace

```text
mloong-agent-kernel/
├─ Cargo.toml
├─ cordis-core/       # 稳定类型、trait、key 和生命周期合同
├─ cordis-runtime/    # Tokio 驱动的合同实现
└─ docs/              # 架构、生命周期、API、测试和集成说明
```

依赖方向固定为：

```text
application / Agent platform
            ↓
      cordis-runtime
            ↓
        cordis-core
```

`cordis-core` 和 `cordis-runtime` 禁止依赖任何 Agent、模型、工具、会话或领域代码。

## 快速验证

在本目录执行：

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

运行最小示例：

```powershell
cargo run -p cordis-runtime --example basic
```

运行基准：

```powershell
cargo bench -p cordis-runtime
```

## 核心概念

- **Runtime**：整个隔离域的生命周期 owner。
- **Scope**：树形生命周期和 Service resolution 边界。
- **Fiber**：Runtime 管理的插件生命周期单元，不是线程或业务任务。
- **Service**：由逻辑 key 标识、按 Scope 就近解析的能力。
- **Invocation**：带稳定 operation identity 的 typed request/response 调用。
- **Event**：进程内短暂事实通知，不承担持久化。
- **Effect**：Fiber 拥有、至多清理一次的生命周期资源。
- **Owned task**：由 Fiber 拥有并随其取消、等待和收敛的异步任务。

## 核心保证

1. 已释放 Scope 中不存在 Active Fiber。
2. 激活提交前，暂存资源对其他 Fiber 不可见。
3. 激活失败不会遗留已提交资源。
4. 每个 owned task 和 Effect 都有唯一 Fiber owner。
5. shutdown barrier 建立后拒绝新增工作。
6. HMR 失败时旧 generation 保持有效；成功时在一个逻辑点切换。
7. 第三方 handler、task 和 lifecycle panic 不破坏 Runtime 状态机。
8. Shutdown incomplete 会报告真实 blocker，并可在 admission 保持关闭的情况下重试收敛。
9. Service symbol、resolution cache、Fiber/Scope arena 均具有显式容量或自动收敛机制。

更完整的说明见：

- [架构](docs/ARCHITECTURE.md)
- [生命周期](docs/LIFECYCLE.md)
- [确定性竞态矩阵](docs/RACE_MATRIX.md)
- [不变量](docs/INVARIANTS.md)
- [API 与扩展](docs/API.md)
- [集成指南](docs/INTEGRATION.md)
- [测试与成熟度](docs/TESTING.md)
- [所有权合同](docs/OWNERSHIP.md)

## 非目标

Kernel 不认识也不提供 Agent、Model、Provider、Prompt、Tool、Session、Memory、Journal、Checkpoint、DurableAction、Planner、Worker 或 TaskGraph。这些概念属于上层平台。

当前版本为 **v1**。Kernel 已进入 contractual maintenance mode：默认只接受 correctness、security、compatibility 和 measured-regression 修复；新增 capability 进入后续版本。
