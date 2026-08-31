# 测试、性能与成熟度

## 本地质量门禁

每次 Kernel 修改至少运行：

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

父级 Agent workspace 也必须运行相同门禁，确保路径依赖和集成没有回归。

## 必测领域

### 生命周期

- Scope/Fiber install、dispose 和 GC；
- dependency loss/restore 与 dispose 竞态；
- activation rollback；
- shutdown admission barrier；
- owned task panic/cancel/join；
- Effect exactly-once cleanup。

### HMR

- 新 revision commit 前失败；
- cutover 与并发 lookup；
-旧 generation 收敛；
- Service/Invocation/Event generation 一致性。

### Dispatch

- typed Service resolution；
- Invocation middleware 与 timeout；
- Event 各 dispatch mode；
- handler panic isolation；
- invocation concurrency limit。

### 资源限制

- scopes、fibers、tasks、handlers、effects 和 depth 上限；
-无效零配置；
- queue/permit cancellation；
- introspection 在 churn 下保持只读和一致。

## 确定性竞态测试

竞态测试应显式控制执行点：

```text
任务 A 到达 test hook X
→ 通知测试线程
→ 测试执行状态变化 Y
→ 释放 A
→ join 全部工作
→ 断言唯一合法结果
```

避免用固定 sleep 推测任务已经运行到某处。

## 性能基线

应持续记录：

- Service cold/warm lookup；
- Invocation dispatch；
- Event dispatch；
- dependency affected-set reconcile；
- Fiber/Scope churn；
- shutdown cancellation；
- HMR cutover。

性能报告至少包含测试环境、样本规模、P50/P95/P99、吞吐和内存变化。没有测量证据时不引入 lock-free 或 unsafe；workspace 已全局禁止 unsafe。

## 长期稳定性

生产成熟门禁还需要：

-数小时到数天的 install/dispose/reload churn；
- RSS/heap/registry/cache/index 收敛曲线；
-随机故障和 panic 注入；
- Linux、Windows 和目标部署平台 CI；
-真实上层平台持续负载。

## 当前成熟度声明

当前 Kernel 已具备清晰边界、明确状态机、结构化 ownership、确定的 linearization point 和较完整的自动测试基础。它适合描述为：

```text
production-oriented, pre-1.0 runtime kernel
```

在长期稳定性、跨平台、公开 API 兼容和生产运行证据完成前，不应描述为 fully mature 或 production-proven。
