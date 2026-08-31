# V2 Optimization 3: Shared-Read Minimal Context Admission

Optimization 3 is accepted. `FiberCell::inner` is now a
`parking_lot::RwLock<FiberMutable>` without splitting the coherent state domain.
Read-only snapshots use short read guards; lifecycle and ownership mutation use
write guards. The production audit classified all 57 access sites as 34 reads and
23 writes, with no read-to-write upgrade, nested same-Fiber lock, or guard across
`.await`.

Context admission now has three private payloads. Base admission retains owner,
Scope, and GenerationLease. Registration admission additionally retains the exact
gate used by admission. Cancellable admission captures the exact cancellation token
only for invoke, sleep, and timeout. All variants share one snapshot helper and
retain the frozen order: upgrade owner, take one coherent Fiber read snapshot, clone
the exact gate, release the guard, run the deterministic test hook, then compete
with draining through `CapabilityGate::try_acquire`. Error precedence and the
GenerationExecution winner boundary are unchanged.

Ten-run release medians (M operations/s) were:

| Real Context | 1 | 2 | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: | ---: |
| shared before | 10.181 | 4.495 | 3.339 | 1.958 | 1.331 |
| shared after | 15.829 | 6.407 | 6.078 | 5.745 | 5.691 |
| independent after | 14.865 | 28.930 | 48.172 | 78.095 | 95.931 |

The 8- and 16-worker shared improvements are 193% and 328%. The 8/4 shape
improves from 0.586 to 0.945 and 16/4 from 0.399 to 0.936. The realistic
five-Symbol shared workload improves from 0.691/0.607/0.436/0.268 to
0.477/0.694/0.787/0.778 M logical operations/s at 1/4/8/16. Its single-worker
regression is workload-specific and is outweighed by the removal of the shared
caller collapse; real single-worker Context admission itself improves 55%.

Thirty-second production reload writer stress completed 278 reloads with eight
readers and 260 with sixteen. Reload P50/P99/max were 0.152/1.03/5.96 ms and
0.211/40.15/55.12 ms respectively, with zero draining-generation accumulation.
The sixteen-reader tail warrants continued observation but is not starvation.
At 0/1/10 reloads per second, eight-worker Service throughput was
3.304/3.241/3.249 M/s and P99 was 6.1/6.2/5.8 us; reload P50/P99 at 1 and 10 Hz
was 0.122/0.332 ms and 0.133/0.496 ms, with no old-generation accumulation.

Optimization 1 and 2 smoke remained healthy: Scenario C was
4.266/4.855/4.724, GE-A 12.755/10.835/9.090, GE-C
9.307/7.628/6.392, PB1 3.568/4.017/3.831, and PB2
3.911/4.276/4.138 M/s at 4/8/16.

The remaining shared base-admission costs are Weak owner upgrade, transient exact
gate Arc clone, and GenerationExecution CAS/refcount traffic. The gate clone is
deliberately retained because release-before-acquire is a frozen race boundary;
Weak ownership is retained because Context must not own Fiber lifetime. No fourth
optimization is admitted from these results. Recharacterize the new path before
proposing any further change.
