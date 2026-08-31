# 独立集成指南

## 作为路径依赖

```toml
[dependencies]
cordis-core = { path = "../core/cordis-core" }
cordis-runtime = { path = "../core/cordis-runtime" }
```

当前上层 Agent workspace 即采用这种方式，因此 Kernel 可以独立验证，同时仍服务于同一仓库中的 Agent 平台。

## 作为独立仓库发布

若未来把 `core/` 单独迁移为 Git 仓库，只需保持两个 crate 的相对目录不变。`cordis-runtime` 对 `cordis-core` 使用相对路径，不依赖父级 Agent workspace。

发布前应补充：

- repository、homepage 和 documentation metadata；
- crate-level changelog；
- semver/API 兼容政策；
- crates.io 或内部 registry 发布流程；
-最低 Rust 版本 CI。

## 上层平台边界

AgentRuntime 可以作为 NativePlugin 安装在 root 或专用 agent Scope 中，并通过 Context 拥有其 supervisor tasks。上层可以使用 Kernel 的生命周期机制，但不得让 Kernel 反向依赖：

```text
AgentDefinition
AgentExecution
ModelProvider
ToolRuntime
Session
Journal
Checkpoint
DurableAction
OrchestrationRun
```

## 升级原则

Kernel API 改动必须至少满足一项：

1. 修复可复现 correctness/lifecycle/concurrency 缺陷；
2. 表达现有 composition 无法表达的不变量；
3. 两个以上真实上层使用者证明需要相同通用机制；
4. benchmark 证明当前实现存在值得修复的瓶颈。

禁止因为单一 Concrete Agent 的领域需求向 Kernel 添加专用概念。
