# V2 Optimization 1: Service cache-hit read fast path

## Decision

**ACCEPTED.** The smallest proposed change removed isolated cache-hit
serialization, preserved the v1 correctness model, did not increase allocation,
and allowed writers to converge under sustained reader load.

## Problem and implementation

At characterization commit `50d914a`, every `ServiceRegistry::resolve` acquired
the global writer, including valid positive and negative cache hits. The new
path uses a short shared guard for a pure cache lookup. It validates epoch and,
for positive entries, location, owner and gate visibility before cloning the
entry. Negative hits return `None`. Missing or stale hints release the reader,
acquire the writer, double-check current state, then retain the existing ancestry
resolution, cache insertion and FIFO eviction behavior. Cache capacity zero
keeps the original writer path.

The registry guard never crosses provider gate admission, retry, user access or
an await. The cache remains a reconstructible hint; selector/gate state and
epoch-invalidated registry truth remain authoritative.

## Correctness evidence

New deterministic tests cover a truth mutation between reader release and
writer acquisition, negative-cache invalidation after provider installation,
and removal of a cached provider. Existing tests cover bounded FIFO behavior,
shadowing, HMR cutover-wins retry, old-handle generation pinning, reload races,
shutdown and fast disposal. All stable and Rust 1.85 gates pass.

## Throughput after (M lookups/s)

Five independent runs per point; table values are medians.

| Workers | A | B | C |
| ---: | ---: | ---: | ---: |
| 1 | 2.841 | 2.974 | 2.966 |
| 2 | 2.162 | 3.463 | 3.550 |
| 4 | 2.302 | 3.067 | 3.777 |
| 8 | 1.974 | 1.299 | 3.938 |
| 16 | 1.181 | 1.212 | 3.728 |

Scenario C moved from 8/4=0.626 and 16/4=0.354 to 1.043 and 0.987. Relative
to before, C changed -1.9%, +32.4%, +120.5% and +269.5% at 1/4/8/16 workers.
Multiple after families had range/median over 10% and are marked noisy in the
run record; the magnitude and direction of isolated C scaling remain clear.

## Controls and residual costs

- Symbol C at 1/4/8/16: 3.352/5.454/5.646/5.540 M/s.
- Key C at 1/4/8/16: 2.966/3.777/3.938/3.728 M/s.
- Cache-off C: 2.727/2.692/1.654/1.009 M/s.
- Depth-32 cache-on C: 1.051/2.363/2.717/2.892 M/s.

The symbol advantage exposes interner lookup as a secondary cost. Cache-off
retains the old writer collapse by design. Depth 32 has lower absolute
throughput but healthy scaling, consistent with ancestry-related CPU cost.

## HMR, writers and allocation

With eight foreground workers, HMR P99 was 16.0/16.9/15.9 us for 0/1/10 reloads
per second. At 1/s and 10/s reload P99 was 300.4 us and 388.1 us, with no old
generation accumulation.

Thirty-second OS-thread stress completed 277 reloads with eight readers and 278
with sixteen. Reload P99 was 421.8 us and 765.9 us; maxima stayed below 1.3 ms.
Install/dispose completed and all runtime counters converged. No writer
starvation was observed. Service allocation remains exactly 1.0000 allocation
and 32 bytes per operation.

## Remaining work

Shared caller/provider controls remain noisy and do not justify changing
GenerationExecution. The key/symbol delta merits profiling of the interner.
Re-characterize both before admitting another production optimization; do not
add Registry sharding or an ancestry cache from this result.
