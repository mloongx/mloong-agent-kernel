# Cordis Kernel v2 conformance manifest

`CONTRACT_MATRIX.md` is the single contract truth source. This manifest explains
how its final classifications map to executable evidence; it does not define a
second contract set.

## Inventory

The matrix has 70 IDs parsed as `[A-Z]+-[0-9]+`: 35 PUBLIC, 12 INTERNAL, 11 HOST,
10 WIRE-CANDIDATE and 2 PLACEHOLDER. Final classifications are 54 READY, 11
INTERNAL-ONLY, 1 CALLER-OBLIGATION, 1 DOCUMENTED-BOUNDARY, 2 PLACEHOLDER and 1
DEFERRED. PARTIAL and UNRESOLVED are zero.

| Matrix class | Evidence authority |
| --- | --- |
| PUBLIC | `cargo test -p cordis-runtime --test conformance` plus focused public integration suites |
| INTERNAL | unit/race tests adjacent to the invariant and workspace tests |
| HOST | host foundation, lifecycle and full ProcessHost integration oracles |
| WIRE-CANDIDATE | ProcessHost transport/invocation oracle and hostile-peer codec tests |
| PLACEHOLDER | documented boundary and rejection/bound tests; no capability claim |

Native admitted PUBLIC has zero UNRESOLVED. `SVC-003` is a caller obligation and
`EVT-001` is a documented non-durability boundary. Reference ProcessHost admitted
HST-001..009 and WIR-002/WIR-004..011 are executable with zero PARTIAL and zero
UNRESOLVED. `SVC-005`, WIR-003, remote Service and remote Event are outside that
admitted scope.

Canonical commands:

```text
cargo test -p cordis-runtime --test conformance
cargo test -p cordis-runtime --test host_conformance
cargo test -p cordis-runtime --test process_host_conformance
cargo test -p cordis-runtime --test process_host_invocation_conformance
cargo test -p cordis-runtime --test process_host_lifecycle_conformance
cargo test -p cordis-runtime --test process_host_full_conformance
```
