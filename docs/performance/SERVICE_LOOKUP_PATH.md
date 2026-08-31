# Post-Optimization Service lookup path

The steady-state `Context::get` path after V2 Optimization 1 is:

| Step | Operation | Synchronization and cost | Sharing scope |
| ---: | --- | --- | --- |
| 1 | `Context::get` -> `try_get` | no synchronization itself | per call |
| 2 | `Context::admit` owner upgrade | weak/strong Arc operation | per Fiber object |
| 3 | caller `FiberCell.inner` snapshot | `parking_lot::Mutex`; reads lifecycle, generation and scope; clones gate and cancellation | shared by Contexts from one caller Fiber |
| 4 | caller gate admission | `GenerationExecution` mutex; increments inflight; Arc clone; lease drop locks again and decrements | shared by one caller generation |
| 5 | `ServiceKey` -> `ServiceSymbol` | global ServiceRegistry `RwLock` reader; `HashMap` lookup; hashes and compares two `Arc<str>` contents plus version | runtime-global interner |
| 6 | `ScopeRegistry::ancestry` | global ScopeRegistry `RwLock` reader; walks parents into an owned `Vec` | runtime-global reader; one allocation plus depth-dependent reallocations |
| 7 | owned-service check | ServiceRegistry reader and HashMap lookup | runtime-global reader |
| 8 | resolution cache hit | ServiceRegistry reader; epoch/location/owner/gate validation; clones `ServiceEntry` and its Arcs | runtime-global reader, independent cache keys |
| 9 | provider gate admission | provider `GenerationExecution` mutex; inflight/service-handle increments; lease drop locks and decrements | shared by one provider generation |
| 10 | `ServiceHandle` construction | Arc clone/downcast; no heap allocation observed beyond ancestry | per returned handle |

No Registry, ScopeRegistry, FiberCell, or gate guard crosses an await. The
ServiceRegistry cache-hit writer from the v1 path is absent. `get_symbol` skips
step 5, which is why a pre-resolved symbol is the natural repeated-lookup form.

The allocation experiment identifies step 6 as the lookup allocation source.
Key and Symbol have identical allocation counts and bytes at every depth.

Repeated hot-path resolution should intern a `ServiceKey` once and reuse its
`ServiceSymbol` while using that Runtime. Symbols are runtime-local: they must
not be persisted, serialized, transferred across hosts, or reused with another
Runtime.
