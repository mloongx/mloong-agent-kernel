# Kernel 1.0 freeze audit

## Safety and dependencies

- Workspace lint policy forbids unsafe Rust. The production crates contain no `unsafe` block or `unsafe impl`.
- Dependencies come only from crates.io and are checked by `cargo audit` and `cargo deny` in CI.
- Stable Rust and the declared Rust 1.85 MSRV are CI matrix entries on Linux and Windows.
- Rust 1.85.0 locally passed workspace check, tests, and all-feature tests. `cargo audit` scanned 99 locked dependencies against 1,226 advisories with no finding. `cargo deny check` passed advisories, bans, licenses, and sources; its informational duplicate warning is the dev-only `criterion`/proc-macro transition between `syn` 2 and 3.

## Panic and invariant sites

Production `expect`/`unreachable` sites are limited to validated transaction topology, bounded two-attempt lookup control flow, default configuration validity, and task-supervisor membership invariants. User-controlled capacity failure is returned as `ResourceLimitExceeded`; symbol exhaustion is no longer an unchecked growth path.

`Context::provide*` cannot grow the interner. It validates `descriptor.provisions` before a lookup-only read of the symbol admitted by lifecycle preparation. The monotonic symbol limit is validated as non-zero and no larger than `u32::MAX`; IDs remain stable and are never reused.

## Task ownership

Production Tokio spawns occur only in the TaskSupervisor, RuntimeWorkerSupervisor, or explicit Runtime-owned Fiber, Scope, and shutdown supervisors. Those lifecycle supervisors publish durable completion independently of caller observer lifetime. Automatic GC is a coalesced RuntimeWorker.

Asynchronous GC registration participates in the same `AdmissionGate` read/write fence as shutdown close. The admission guard covers `gc_state` mutation and synchronous worker registration without crossing an await. Shutdown-first requests are rejected before state mutation; GC-first workers are visible to the shutdown worker drain.

## Locks and awaits

Parking-lot guards protect short synchronous registry or record mutations and are released before await. The deliberate awaitable lock is the Tokio per-Fiber lifecycle mutex. Shutdown completion waits and test hooks hold no synchronous correctness lock.

## Public contract

The public lifecycle surface consists of Runtime install/reload/dispose/shutdown operations, detailed outcome types, Context, non-cloneable ServiceHandle, health/snapshot structures, configuration, and core identity/key/error types. Registry cells, generation leases, prepared revisions, mutable transaction state, and durable observation payloads remain private.

## Freeze gates

The local release gate is fmt, strict all-target/all-feature clippy, debug tests, all-feature tests, release tests, no-default-feature compilation, and warning-free rustdoc. Supply-chain checks are enforced in CI; local installation can be skipped only when the CI gate remains required.

Current decision: **CORDIS KERNEL v1.0 FROZEN**. H-001 through H-024 have source, deterministic test, resource-convergence, MSRV, supply-chain, and release-gate evidence. Further architectural work is deferred to V2 performance characterization.
