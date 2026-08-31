# Cordis Kernel v2 crash and observer-drop matrix

Durable Action C0-C13 is an AgentLayer protocol boundary, not Kernel code. Its
required guarantee is duplicate-side-effect prevention or reconciliation using
durable intent/result identity; Kernel Effects and remote Invocation do not
provide external exactly-once execution.

| Case | Contract / evidence | Expected terminal outcome | Status |
| --- | --- | --- | --- |
| Durable Action C0-C13 | AgentLayer boundary; lifecycle durability docs | reconcile ambiguous external effects; never infer exactly-once from Kernel completion | BOUNDARY |
| install observer drop | OWN-001 native conformance | Runtime-owned commit/rollback continues | PASS |
| reload observer drop | OWN-001/HMR public oracle | transaction and cleanup continue | PASS |
| dispose observer drop | OWN-002/DSP-001 | immutable registered completion | PASS |
| shutdown observer drop | OWN-002/SHD-002 | attempt continues under Runtime ownership | PASS |
| Host handshake/load drop | HST-009 ProcessHost oracle | child and actors reaped; no Runtime acceptance | PASS |
| Host start crash | HST-002/HST-004 | rollback before commit or exact-generation convergence after commit | PASS |
| remote invoke crash | HST-004/WIR-005 | one local terminal Host failure; no replay | PASS |
| cancel/result race | HST-005/WIR-008 | one local winner; late result discarded | PASS |
| deadline/result race | HST-005/WIR-007 | one local winner; Host cannot extend deadline | PASS |
| HMR precommit crash | HST-007 | old generation remains authoritative | PASS |
| HMR postcommit crash | HST-004/HST-007 | new generation remains committed and converges | PASS |
| old-generation drain crash | HST-007 | old work fails locally; replacement unaffected | PASS |
| Host failure vs dispose | HST-004 | one Runtime-owned disposal convergence | PASS |
| Host failure vs shutdown | HST-008 | one bounded shutdown attempt | PASS |
| shutdown force kill/reap | HST-008 | cleanup issue or Complete; no live blocker after reap | PASS |
| unreaped HostedExecution | SHD-003/HST-008 | concrete `HostedExecution` blocker | PASS |
| shutdown retry | SHD-003 | admission stays closed; retry converges after blocker clears | PASS |
