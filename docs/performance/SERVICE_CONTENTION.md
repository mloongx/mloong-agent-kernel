# Service resolution contention attribution

## Optimization 1 closure

The Phase 0.3 attribution was correct: a valid resolution-cache hit took the
single global `ServiceRegistry` writer. Optimization 1 now checks a cache hit
under a shared reader, validates epoch, location, owner and gate visibility,
clones the entry, and releases the guard. A miss or stale hint drops the reader,
takes the writer, double-checks the cache, then performs the unchanged ancestry
walk and bounded-FIFO mutation. A valid negative cache entry remains distinct
from a miss.

The frozen before results (commit `50d914a`, M lookups/s, five-run medians) were:

| Workers | A shared/shared | B independent/shared | C independent/independent |
| ---: | ---: | ---: | ---: |
| 1 | 2.937 | 2.877 | 3.025 |
| 2 | 2.709 | 3.073 | 3.292 |
| 4 | 2.374 | 2.660 | 2.853 |
| 8 | 1.696 | 1.673 | 1.786 |
| 16 | 1.030 | 1.005 | 1.009 |

After the read fast path, using the same machine and method:

| Workers | A | B | C |
| ---: | ---: | ---: | ---: |
| 1 | 2.841 | 2.974 | 2.966 |
| 2 | 2.162 | 3.463 | 3.550 |
| 4 | 2.302 | 3.067 | 3.777 |
| 8 | 1.974 | 1.299 | 3.938 |
| 16 | 1.181 | 1.212 | 3.728 |

Scenario C is decisive: 8/4 improved from 0.626 to 1.043 and 16/4 from 0.354
to 0.987. Its 8- and 16-worker throughput improved 120.5% and 269.5%, while one
worker changed -1.9%. This eliminates the independent caller/provider collapse.
A and especially B retain intentionally shared caller/provider admission;
several after runs were noisy (range/median over 10%), so their medians are
controls, not evidence for another production change.

## Remaining controls

Cache-off Scenario C still collapses at 2.727/2.692/1.654/1.009 M/s for
1/4/8/16, while cache-on is 2.966/3.777/3.938/3.728. At depth 32, cache-on C is
1.051/2.363/2.717/2.892 M/s. The lower absolute rate reflects ancestry-related
CPU work, but the writer collapse is absent. Symbol C is
3.352/5.454/5.646/5.540 M/s; its roughly 43-44% advantage over key at 4/8 makes
interner lookup a visible secondary cost, not an admitted optimization.

Eight-worker HMR foreground P99 was 16.0 us at 0 reload/s, 16.9 us at 1/s and
15.9 us at 10/s, improving over the prior approximately 32.1/34.5/34.8 us. A
30-second OS-thread writer stress completed 277 and 278 reloads at 8 and 16
readers. Reload P99 was 421.8 us and 765.9 us, max 985 us and 1.285 ms, with no
generation or worker accumulation. No writer starvation was observed.

The 16-worker Tokio HMR harness sometimes scheduled zero reloads because all
executor workers were occupied by foreground tasks. The independent OS-thread
stress avoids this harness limitation and directly verifies Registry writer
progress.

## Attribution result

Optimization 1 is **ACCEPTED**. The global writer is no longer the dominant
cache-hit bottleneck in isolated Scenario C. Registry sharding is not justified.
Next characterize shared caller/provider admission and key-path interner cost;
no second production optimization is admitted yet.

## Phase 1.1 follow-up

The post-optimization matrix is recorded in
`V2_PHASE_1_1_SERVICE_RECHARACTERIZATION.md`. Shared caller and shared provider
generation paths both reproduce a collapse, while PB1 same-entry and PB2
different-entry results are essentially equal. The provider generation is the
strongest structural candidate, but isolated acquire/drop evidence and stable
critical-point variance are still missing. Optimization 2 remains DEFER.
