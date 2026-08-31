# V2 Phase 1.4: Context admission cost decomposition

## Decision

**Optimization 3 is admitted for a future implementation batch.** The candidate
is a narrow Fiber admission read-path redesign: shared-reader synchronization
plus operation-specific minimal coherent payload capture. Base admission should
not unconditionally clone CancellationToken or retain a gate Arc. Registration
and cancellable variants retain only what they consume. This phase changes no
production code and does not admit Fiber state splitting, ArcSwap, strong
Context ownership, or a raw Mutex-to-RwLock substitution.

## Real cost and usage model

`Context::admit` performs Weak owner upgrade, exclusive Fiber lock, scalar
generation/state reads, gate Arc clone, Scope copy, CancellationToken clone,
unlock, packed atomic gate acquire, GenerationLease ownership, and stack-only
ContextAdmission construction. Shared cache-line write candidates are the Weak
control block, Mutex state, gate Arc control block, CancellationToken shared
state, and GenerationExecution atomic. Scalars and result construction do not
independently write shared cache lines or allocate.

Twenty-two direct admission sites were audited. Every current site transiently
owns the gate so acquire can occur after unlocking Fiber, but only four paths
retain/use it afterward: `provide_arc`, `on`, `handle_invocation`, and
`invocation_middleware`. Only `invoke_inner`, `sleep`, and `timeout` consume the
captured cancellation.

| Method/path | owner | scope | persistent gate | cancellation | lease/base only |
|---|---:|---:|---:|---:|---:|
| `scope`, `parent` | no | yes | no | no | yes |
| `root` | no | no | no | no | yes |
| `provide_arc` | yes | yes | yes | no | no |
| `try_get`/`get`, `get_symbol`, `contains` | no | yes | no | no | yes |
| `effect` | yes | yes | no | no | no |
| event/invocation registration | yes | mixed | yes | no | no |
| `invoke_inner` dispatch admission | yes | yes | no | yes | no |
| `spawn`, `create_scope` | yes | yes | no | no | no |
| `sleep`, `timeout` | no | no | no | yes | no |
| `interval`, event dispatch family | no | no | no | no | yes |

Most hot Service/event operations pay for cancellation they never use and keep
a generic gate field after its only required use. Lazy cancellation must still
be cloned from the original coherent snapshot; re-locking later could mix
generation and cancellation.

## Pure lock matrix

Five Copy scalars, no Arc/token/allocation; ten-run median M operations/s:

| Workers | Shared Mutex | Shared RwLock | Independent Mutex | Independent RwLock |
|---:|---:|---:|---:|---:|
| 1 | 86.503 | 64.098 | 87.649 | 63.138 |
| 2 | 54.199 | 25.120 | 109.327 | 110.189 |
| 4 | 20.431 | 17.507 | 57.312 | 44.475 |
| 8 | 5.224 | 16.029 | 100.287 | 84.564 |
| 16 | 8.179 | 14.028 | 184.118 | 155.861 |

Shared Mutex 8/4 and 16/4 are 0.256/0.400; RwLock is 0.916/0.801. The Mutex has
the first admission cliff. Scalar RwLock is viable and did not cause the full
Phase 1.3 shadow to plateau near 2.7 M/s.

## Reference-count decomposition

Ten-run median M clone/drop or upgrade/drop operations/s:

| Workers | Shared Arc<()> | Shared gate Arc | Shared cancellation | Both/no lock | Shared Weak Fiber | Independent cancellation | Independent Weak Fiber |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 83.384 | 81.945 | 29.540 | 21.762 | 61.970 | 29.433 | 61.443 |
| 2 | 45.392 | 44.633 | 9.350 | 8.357 | 30.065 | 56.769 | 121.877 |
| 4 | 40.509 | 40.153 | 4.829 | 3.793 | 28.047 | 99.821 | 238.946 |
| 8 | 36.170 | 36.840 | 4.197 | 3.363 | 23.447 | 130.878 | 390.005 |
| 16 | 30.573 | 30.610 | 4.213 | 3.405 | 15.168 | 186.794 | 472.567 |

Independent Arc<()> is 83.727/157.426/247.783/182.915/363.270 and independent
gate Arc is 83.570/167.148/320.440/483.128/557.489. Arc<()> and real gate curves
match: clone cost is the control block, not gate size. Shared Arc remains 30.6
M/s at 16 workers. CancellationToken is the dominant payload collapse, plateauing
near 4.2 M/s while independent tokens reach 186.8 M/s. Both clones without a
lock yield 3.4 M/s and explain most of the Phase 1.3 full-shadow ceiling.

Weak upgrade is material but not the first cliff. It is an intentional cost of
the frozen rule that Context does not own Fiber lifetime; storing a strong Fiber
Arc is forbidden.

## Lock plus payload layers

| Workers | M scalar | M gate | M cancel | M full | R scalar | R gate | R cancel | R full |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 85.507 | 43.206 | 22.640 | 17.705 | 63.977 | 36.853 | 19.741 | 16.537 |
| 2 | 55.431 | 27.697 | 14.712 | 10.508 | 24.085 | 15.474 | 7.138 | 7.078 |
| 4 | 19.965 | 10.231 | 5.064 | 3.579 | 16.122 | 12.410 | 3.487 | 3.242 |
| 8 | 5.904 | 3.350 | 2.602 | 2.296 | 17.701 | 10.678 | 3.405 | 2.779 |
| 16 | 8.624 | 3.558 | 1.964 | 1.572 | 12.730 | 9.980 | 3.347 | 2.733 |

With reader synchronization, gate clone is bounded while cancellation drops
16-worker throughput from 12.730 to 3.347 M/s. Full payload falls only another
18%. Cancellation is the dominant rich-payload component.

## Cumulative real-Fiber model

| Layer | 1 | 4 | 8 | 16 |
|---|---:|---:|---:|---:|
| C0 Weak owner | 65.204 | 29.279 | 26.161 | 17.790 |
| C1 + real Mutex snapshot | 37.847 | 16.799 | 4.998 | 4.002 |
| C2 + gate clone | 30.154 | 8.151 | 3.592 | 2.556 |
| C3 + cancellation | 14.366 | 4.553 | 2.302 | 1.511 |
| C4 + generation lease | 11.574 | 3.541 | 1.971 | 1.343 |
| Real `Context::scope` | 10.181 | 3.339 | 1.958 | 1.331 |

The production-component model matches real Context within about 1-14% and
almost exactly at 4/8/16. The first cliff is exclusive Fiber synchronization;
cancellation is the largest later amplifier; gate ownership and Weak upgrade
follow. Current shared GenerationExecution acquire/drop is
46.218/12.951/10.933/9.022 M/s at 1/4/8/16. C3-to-C4 costs about 11% at 16, so
Optimization 2 leaves a smaller residual rather than the root cost.

## Borrowed-gate proxy

| Workers | Clone then acquire | Borrow/acquire under read | Gain |
|---:|---:|---:|---:|
| 1 | 24.805 | 29.416 | 18.6% |
| 4 | 7.190 | 8.922 | 24.1% |
| 8 | 6.489 | 7.975 | 22.9% |
| 16 | 6.148 | 6.853 | 11.5% |

`try_acquire` is synchronous and nonblocking. The proposed order is Fiber-read
then GenerationExecution atomic; no reverse GenerationExecution-to-Fiber lock
edge or await exists. If acquire wins, drain includes the lease; if selector
cutover/drain wins, the borrowed gate rejects. A writer is delayed only for the
short read/atomic interval. This changes the current unlock-before-acquire
sequence, so deterministic HMR/reactivation/disposal races are mandatory in the
implementation batch, but its winner proof is clean.

## Candidate and deferred alternatives

A future internal design may distinguish base admission (current Scope and exact
lease), registration admission (also retain gate), and cancellable admission
(also clone cancellation from the same snapshot). Gate borrowing applies only
when no persistent gate is needed.

Immutable/ArcSwap publication is not next: scalar RwLock is healthy, and
`Arc<Snapshot>::clone` could move contention to a new shared Arc control block.
ArcSwap-style guarded loads remain possible later. Fiber state split, registry
sharding, interner, ancestry, GC and DependencyGraph redesign remain deferred.

## Real correlation and regression smoke

Real shared Context is 10.181/4.495/3.339/1.958/1.331 M/s at 1/2/4/8/16.
The realistic five-Symbol shared caller is 0.691/0.607/0.436/0.268 M logical/s
at 1/4/8/16.

Optimization 1 Scenario C is 5.151/5.667/5.210 M/s at 4/8/16. Optimization 2
GE-A is 12.951/10.933/9.022, GE-C is 9.805/7.963/6.007, PB1 is
3.771/4.322/4.041, and PB2 is 3.942/4.410/4.300 M/s. Both remain healthy.

Before production acceptance, add deterministic tests for borrowed admission
versus reload cutover, reactivation replacement, same-generation Scope
relocation, disposal, stale Context, cancellation coherence, registration gate
retention, and no guard across await. Repeat the decomposition and real curves
as before/after gates.
