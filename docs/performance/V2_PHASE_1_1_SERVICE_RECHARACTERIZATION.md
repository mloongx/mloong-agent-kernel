# V2 Phase 1.1: post-optimization Service recharacterization

## Decision

No V2 Optimization 2 is admitted. The new controls rank shared caller and
provider generation admission above interner and ancestry costs, but the
primitive acquire/drop benchmark required for a production GenerationExecution
proposal cannot be built from the legal public path without exposing internal
state. Critical shared-provider points also remain noisy. The correct result is
targeted follow-up characterization, not implementation.

The baseline is an annotated tag. Its tag-object ID is `f579ee2`; peeling it
with `^{}` resolves correctly to frozen commit `12de9831`. There is no metadata
discrepancy.

## Caller and provider controls

Release build, OS threads, depth 1, cache 4096, pre-resolved Symbol, ten runs,
200,000 operations per worker. Values are median M lookups/s.

| Workers | CA independent caller/provider | CB shared caller/independent provider | PB1 same provider/same symbol | PB2 same provider/different symbols |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3.428 | 3.336 | 3.320 | 3.384 |
| 2 | 4.261 | 3.264 | 4.026 | 4.057 |
| 4 | 5.133 | 3.053 | 3.584 | 3.823 |
| 8 | 5.639 | 1.878 | 1.566 | 1.594 |
| 16 | 5.315 | 1.151 | 1.120 | 1.127 |

CB/CA is 0.973/0.766/0.595/0.333/0.217. Shared caller is therefore a
repeatable hotspot. It reintroduces both the FiberCell mutex and caller
GenerationExecution, so this experiment cannot distinguish them.

PB1 and PB2 have effectively the same collapse. Different ServiceEntry objects
do not restore scaling; their shared provider gate/GenerationExecution is the
common structural point. However, 8/16-worker ranges remain above 10%, and a
clean acquire/drop primitive is unavailable without a production-facing bench
API. Provider GenerationExecution is a HIGH-confidence hypothesis, not an
admitted optimization.

## Key and interner controls

Five-run medians, independent caller/provider, depth 1:

| Key name length | 1 worker | 4 | 8 | 16 |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 2.978 | 3.863 | 4.035 | 3.897 |
| 32 | 2.941 | 3.846 | 4.068 | 3.839 |
| 128 | 2.835 | 3.943 | 4.040 | 3.792 |
| 512 | 2.337 | 3.694 | 3.977 | 3.767 |
| Symbol control (32) | 3.360 | 5.325 | 5.642 | 5.605 |

At length 32, same-key medians were 2.952/3.684/3.957/3.733 M/s and many-key
medians 3.091/3.920/4.104/3.824. Their scaling is alike and many keys are not
slower. Length 8 through 128 has a small effect; 512 characters costs about 21%
at one worker but little at high concurrency. The evidence supports a fixed
global-reader/HashMap lookup cost plus hashing/equality CPU, not a new interner
writer-style scalability collapse.

The public API already documents `ServiceSymbol` as runtime-local and intended
for hot paths. Reusing a pre-resolved Symbol is recommended for repeated access;
`ServiceKey` remains the stable, human/configuration and convenience boundary.
No API change or interner redesign is justified.

## Ancestry and allocation

Single-worker Symbol medians by depth:

| Depth | M lookups/s | ns/op | allocation behavior |
| ---: | ---: | ---: | --- |
| 1 | 3.358 | 297.8 | 1 allocation, 32 B |
| 2 | 3.204 | 312.2 | 1 allocation, 32 B |
| 4 | 2.699 | 370.5 | 1 allocation, 32 B |
| 8 | 2.203 | 453.9 | 1 allocation, 1 realloc, 64 B |
| 16 | 1.701 | 587.9 | 1 allocation, 2 reallocs, 128 B |
| 24 | 1.470 | 680.1 | 1 allocation, 3 reallocs, 256 B |
| 32 | 0.971 | 1030.1 | 1 allocation, 3 reallocs, 256 B |

Key and Symbol allocation results are identical. This directly attributes the
one allocation and depth-dependent reallocations to the owned ancestry `Vec`,
not ServiceHandle, interner, or Context admission. Cost increases with depth as
expected. No ancestry cache is justified by current absolute cost or scaling.

## Realistic five-lookups-per-operation workload

Logical operations/s, five-run medians:

| Path | 1 | 4 | 8 | 16 |
| --- | ---: | ---: | ---: | ---: |
| shared caller / Key | 0.608M | 0.522M | 0.425M | 0.262M |
| shared caller / Symbol | 0.692M | 0.577M | 0.398M | 0.238M |
| independent caller / Key | 0.613M | 0.773M | 0.836M | 0.768M |
| independent caller / Symbol | 0.675M | 1.015M | 1.116M | 1.082M |

The complete Context-to-ServiceHandle path reproduces both conclusions:
independent lanes scale, shared callers collapse, and Symbol reuse helps when
caller admission is not dominant.

## Optimization 1 regression

Fresh key-path Scenario C cache-on medians at 1/4/8/16 were
3.058/3.684/4.022/3.808 M/s (8/4=1.092, 16/4=1.034). Cache-off was
3.074/2.891/1.728/0.990 M/s. Optimization 1 remains effective.

Eight-worker HMR P99 at 0/1/10 reload/s was 15.8/17.5/16.9 us, with no old
generation accumulation. The 1/s run contained one approximately 2.0 s reload
outlier from foreground Tokio scheduling; the 10/s run completed 75 reloads
with reload P99 920.6 us. This is consistent with the previously documented
harness scheduling limitation rather than Registry writer starvation.

## Confidence and next research

1. Shared caller combined admission: HIGH. CB and realistic workloads repeat it.
2. Provider GenerationExecution: HIGH hypothesis, but insufficient admission
   evidence until isolated acquire/drop is measured with an internal bench-only
   target and variance is controlled.
3. Interner lookup/hash cost: HIGH as a secondary fixed cost; documentation-level
   Symbol reuse is currently the proportionate response.
4. FiberCell versus caller GenerationExecution: LOW individually because CB
   reintroduces both and no clean primitive isolation was added.

CPU/Idle sampling was not retained: these sub-second/multi-second child
processes did not provide a reliable synchronized Windows counter sample in the
current harness. Prior evidence already disproved CPU saturation for the old
collapse, but no new Phase 1.1 CPU claim is made.
