# Deterministic race matrix

Cordis tests lifecycle races with barriers, semaphores, hooks, and durable completion observations. Sleeps are not used to decide the winner of a correctness race.

| Boundary | Competing operations | Required result |
| --- | --- | --- |
| activation commit | install vs shutdown/dispose | one admission winner; losing install rolls back before publication |
| selector cutover | reload vs reload/dispose/shutdown | one CAS winner; pre-CAS work is reversible and post-CAS cleanup is forward-only |
| Scope cutover | reload metadata vs Scope disposal | committed Fiber metadata is visible before disposal may snapshot topology |
| generation drain | Context/Invocation/Event/ServiceHandle vs disposal | admission-first work may finish; drain-first work cannot enter |
| disposal publication | waiter/caller drop/GC vs finalizer | synchronous indexes converge before immutable completion is published |
| shutdown completion | concurrent callers/caller drop/retry | callers share one attempt; incomplete attempts may retry without reopening admission |
| task deadline | task completion vs abort deadline | terminal task state is recorded exactly once and boundary completion is not misclassified |
| automatic GC | terminal Fiber/Scope vs snapshots and waiters | arena reclamation preserves durable completion and detaches plugin ownership exactly once |
| GC registration fence | post-publish finalizer vs shutdown close | GC-first registration is drained; shutdown-first rejection cannot create a post-Complete worker or leave scheduled state |
| service lookup | cache/interner churn vs HMR/Scope shadowing | lookup remains correct while cache and symbol counts stay within configured bounds |
| service publication | undeclared/full-capacity key vs lifecycle-prepared symbol | undeclared publication is typed and allocation-free; declared publication reuses its stable admitted ID |

The implementation tests these boundaries in `cordis-runtime/src/runtime/tests.rs` and the public behavior in `cordis-runtime/tests`. Resource churn tests additionally assert convergence of Fiber, Scope, generation, task, worker, staging, cache, symbol, and plugin-reference counters.
