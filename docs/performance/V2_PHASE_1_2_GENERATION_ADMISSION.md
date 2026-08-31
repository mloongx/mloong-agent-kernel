# V2 Phase 1.2: generation admission primitive isolation

## Decision

V2 Optimization 2 is **ADMITTED**, but not implemented in this batch. Direct
crate-internal release characterization proves that shared
`GenerationExecution.state` synchronization collapses while independent
executions scale. The service acquire path has the same curve as normal acquire,
and PB1/PB2 retain the same shared-provider collapse. Confidence is VERY HIGH.

FiberCell snapshot locking is a second, independently proven shared-caller
hotspot. It is not selected first because GenerationExecution affects both
caller and provider admission and therefore explains more of the end-to-end
evidence. The optimizations must remain separate.

## Harness

The ignored `generation_admission_primitive_characterization` test lives inside
the runtime test module and is compiled only under `cfg(test)`. It exposes no
public API. OS threads are created before the timed region and synchronize with
ready/start/finish barriers. Each point uses ten independent runs. Shared cases
use at least 500,000 operations per worker; noisy 8-worker GE points were rerun
at 2,000,000 operations per worker, reducing variation to 0.9-2.5%.

The Context benchmark disables the existing global test race-hook lookup using
a test-only atomic flag. Without this, `cfg(test)` instrumentation—not the
production path—would add an unrelated global mutex to every admission.

## Primitive results

Values are median M acquire/drop or snapshot operations per second.

| Workers | GE-A shared normal | GE-B independent normal | GE-C shared service | GE-D independent service |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 29.313 | 31.141 | 28.062 | 30.298 |
| 2 | 17.414 | 59.353 | 16.983 | 59.217 |
| 4 | 5.991 | 105.291 | 6.028 | 95.417 |
| 8 | 4.544 | 135.235 | 4.515 | 138.457 |
| 16 | 2.538 | 197.804 | 2.530 | 202.311 |

GE-A 8/4 is 0.758 and 16/4 is 0.424; GE-B is 1.284 and 1.879. GE-C is
0.749/0.420 and GE-D is 1.451/2.120. At 16 workers the shared/independent ratio
is about 1.3% for both normal and service paths. Updating `service_handles`
adds no material scaling change relative to normal acquire/drop.

| Workers | FC shared | FC independent | Context shared | Context independent |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 18.860 | 19.828 | 9.579 | 9.621 |
| 2 | 10.096 | 37.989 | 5.084 | 18.842 |
| 4 | 5.397 | 69.573 | 3.213 | 24.447 |
| 8 | 2.176 | 90.382 | 1.525 | 43.359 |
| 16 | 2.443 | 128.475 | 1.931 | 52.655 |

FC shared 8/4 is 0.403 and 16/4 is 0.453, while independent is 1.299/1.847.
Context shared is 0.475/0.601 versus independent 1.774/2.154. Both primitive
locks contribute to shared caller collapse. Context points remain scheduler
noisy, but the shared/independent direction is unambiguous and agrees with the
primitive curves.

## Provider revalidation

Long Symbol-only PB results, ten-run medians:

| Workers | PB1 same symbol | PB2 different symbols |
| ---: | ---: | ---: |
| 1 | 3.381 | 3.351 |
| 2 | 3.720 | 4.238 |
| 4 | 3.362 | 3.755 |
| 8 | 1.418 | 1.473 |
| 16 | 1.810 | 1.823 |

PB1 8/4 and 16/4 are 0.422/0.538; PB2 is 0.392/0.485. The 8-worker points
remain noisy, likely reflecting hybrid-core scheduling, but every run family
retains severe shared-provider loss, PB1 and PB2 match each other, and the
16/4 ratios closely match GE-C's 0.420. Combined with direct primitive evidence
and source structure, this satisfies end-to-end correlation.

## Correctness baseline

Existing acquire-wins/drain-wins tests remain authoritative. A new 1,000-iteration
race repeatedly starts draining with a lease held, races waiter registration
against last drop, and requires the waiter to finish. The service primitive
harness asserts `inflight == 0` and `service_handles == 0` after every stress
run. No lost wakeup or accounting error was observed.

## Optimization 2 proposal

Candidate: reduce accepting-state `GenerationExecution` acquire/drop
synchronization while preserving the exact drain contract.

The design must give acquire and `begin_draining` a single winner. A naive
`AtomicU8 state + AtomicUsize inflight` is rejected because an acquire can read
Accepting, lose to drain, then increment after the drain boundary. The leading
design candidate is a packed atomic containing state bits and inflight count,
updated by CAS for acquire, drain transition, and last drop. A slow-path mutex
and `Notify` may remain for waiters and diagnostics.

Required proof obligations:

1. Successful acquire CAS is the acquire-wins linearization point.
2. Successful Accepting-to-Draining CAS is the drain-wins point and rejects all
   later acquires.
3. The packed inflight count makes drain observe every earlier winning acquire.
4. Last-drop transition to Drained publishes before waking waiters.
5. Waiter registration plus state recheck prevents lost wakeups.
6. Snapshot returns a coherent state/inflight pair.
7. `service_handles` remains exactly-once diagnostic accounting. It may use a
   separate atomic only if its required consistency relative to packed state is
   specified and tested; otherwise it stays on the slow/coherent path.

Loom or equivalent model testing is recommended for acquire-vs-drain,
multi-acquire/drop, last-drop-vs-waiter, and service-handle accounting. Whether
to add loom as a dev dependency is deferred to the implementation batch.

The locked before suite for Optimization 2 is GE-A/B/C/D, FC shared/independent,
Context shared/independent, PB1/PB2, and the Phase 1.1 realistic shared-caller
workload.
