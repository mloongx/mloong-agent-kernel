# V2 Optimization 2: GenerationExecution atomic admission fast path

## Decision

**ACCEPTED.** A packed atomic replaces the per-generation mutex on accepting
acquire/drop. Shared normal/service admission improved substantially at 4-16
workers, independent 16-worker throughput did not regress after preventing
cross-generation false sharing, shared-provider Service lookup recovered, and
all drain, HMR, shutdown and MSRV gates pass.

## State representation

`GenerationExecution.word` is an `AtomicUsize`. Its low two bits encode
Accepting=0, Draining=1 and Drained=2; 3 is reserved. Remaining bits encode the
inflight count, giving a maximum of `usize::MAX >> 2`. Overflow is an internal
invariant panic rather than silent wrap or a public error-model change.

`service_handles` is a separate `AtomicUsize`. It is an exact diagnostic subset,
not drain authority. `Notify` is wake-only; the packed word is state truth. The
object is aligned to 64 bytes because removing the larger mutex exposed severe
false sharing between independently allocated generations. Alignment restored
GE-B/D independent scaling.

## Linearization and winner proof

- Acquire linearizes at the successful Accepting/N -> Accepting/N+1 CAS.
- Drain linearizes at the successful Accepting/N -> Draining/N or Drained/0 CAS.
- Ordinary drop linearizes at its N -> N-1 CAS.
- Last drop linearizes at the single Draining/1 -> Drained/0 CAS.
- Snapshot is one atomic load and therefore returns a coherent state/count pair.

Acquire and drain operate on the same word. If acquire CAS wins, drain must
include that increment. If drain CAS wins, the state bits are no longer
Accepting and every later acquire fails. A late increment after a winning drain
is therefore impossible.

The last drop publishes Drained/0 before notifying. `drain_until` creates its
notification future before reloading the word. A notification occurring before
or during registration is harmless because the atomic recheck observes
Drained/0. Notification is never used as truth.

## Memory ordering

- Initial and retry loads: Acquire.
- Acquire CAS: AcqRel success, Acquire failure.
- Drain CAS: AcqRel success, Acquire failure.
- Drop CAS: AcqRel success, Acquire failure.
- Snapshot and service-handle diagnostic loads: Acquire.
- Service-handle increment: Relaxed after packed acquire ownership is won.
- Service-handle decrement: Release before packed inflight decrement.

The successful acquire observes generation publication and owns an inflight
slot. Release on drop publishes lease-covered work; a drainer's Acquire load of
Drained observes the last-drop release sequence. For service leases, subset
decrement is sequenced before the packed release, so observing terminal packed
convergence cannot leave a corresponding service count outstanding.

## Service-handle accounting

Acquire order is packed inflight then service count. Drop order is service count
then packed inflight. Each lease carries its service/non-service identity and
updates exactly once. Stress asserts final inflight=0 and service_handles=0.
Shutdown and health already read these through separate APIs, so no nonexistent
three-field atomic snapshot contract is introduced.

## Testing strategy

Loom was not added. Sharing the exact production algorithm with Loom would have
required abstracting atomic and Notify primitives or duplicating the algorithm,
both reducing local auditability. Instead the batch uses the production code in:

- deterministic acquire-wins and drain-wins tests;
- 32-thread normal and service drain races;
- zero successful post-drain acquisitions;
- exact service-handle accounting checks;
- 1,000 last-drop versus waiter-registration races;
- HMR cutover, old-handle pin, shutdown blocker/retry and full concurrency suites.

Model testing remains recommended if the state machine is generalized later.

## Primitive before and after

M acquire/drop pairs/s, ten-run medians:

| Workers | GE-A before | GE-A after | GE-B before | GE-B after | GE-C before | GE-C after | GE-D before | GE-D after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 29.313 | 51.792 | 31.141 | 54.327 | 28.062 | 35.103 | 30.298 | 34.944 |
| 2 | 17.414 | 16.139 | 59.353 | 103.919 | 16.983 | 10.726 | 59.217 | 69.363 |
| 4 | 5.991 | 12.806 | 105.291 | 187.608 | 6.028 | 9.471 | 95.417 | 127.257 |
| 8 | 4.544 | 10.567 | 135.235 | 231.982 | 4.515 | 7.732 | 138.457 | 158.652 |
| 16 | 2.538 | 8.553 | 197.804 | 304.587 | 2.530 | 6.092 | 202.311 | 208.909 |

GE-A 16 improves 237%; GE-C 16 improves 141%. Independent GE-B/D 16 changes
+54%/+3%. Shared service at two workers regresses because the packed and
diagnostic atomics still transfer cache-line ownership, but 4-16 worker results
materially improve and the old mutex collapse is removed.

## System controls

PB1 at 1/2/4/8/16 is 3.566/4.368/3.850/4.146/3.957 M/s. PB2 is
3.576/4.511/4.289/4.475/4.204 M/s. Relative to Phase 1.2, 8-worker throughput
improves 193-204% and 16-worker throughput 119-131%.

The realistic five-Symbol-lookup shared-caller workload is
0.629/0.575/0.398/0.255 M logical ops/s at 1/4/8/16. Its limited change confirms
that FiberCell is now the dominant shared-caller limiter. The independent
control remains 0.697/1.003/1.099/1.046 M logical ops/s.

Generation drain drop/finalize medians at 0/1/8/32/128/512/1000 retained handles
remain within the frozen range. At 1,000, drop is 21.1 us and finalization 92 us.

Eight-worker HMR foreground P99 is 10.3/11.2/11.3 us for 0/1/10 reloads/s, with
no old-generation accumulation. The Tokio 10/s harness again contains a roughly
2 s scheduling outlier and completed 37 reloads. Independent 30-second OS-thread
stress completed 277 reloads at 8 readers and 275 at 16, with no draining
generation or worker accumulation; reload P99 was 4.75/5.07 ms.

## Remaining work

Do not optimize FiberCell in this batch. Recharacterize the post-Optimization-2
caller path before admitting Optimization 3. Registry sharding, interner
redesign and ancestry caching remain deferred.
