//! Canonical executable oracle for the admitted reference `ProcessHost` contract.
//!
//! This target deliberately composes the public transport, invocation, and
//! lifecycle suites so one command exercises the complete frozen surface.

#[path = "process_host_invocation_conformance.rs"]
mod invocation;
#[path = "process_host_lifecycle_conformance.rs"]
mod lifecycle;
#[path = "process_host_conformance.rs"]
mod transport;

#[test]
fn wire_schema_contains_no_runtime_authority_types() {
    let schema = include_str!("../src/host/protocol.rs");
    for forbidden in [
        "ScopeId",
        "FiberId",
        "GenerationId",
        "InvocationId",
        "InvocationMetadata",
        "ServiceSymbol",
        "Context",
    ] {
        assert!(
            !schema.contains(forbidden),
            "private wire schema must not contain Runtime authority type {forbidden}"
        );
    }
}

#[test]
fn process_host_code_has_no_direct_runtime_lifecycle_authority() {
    let implementation = include_str!("../src/host/process.rs");
    for forbidden in [
        "FiberCell",
        "RuntimeInner",
        "ScopeRegistry",
        "GenerationSelector",
    ] {
        assert!(
            !implementation.contains(forbidden),
            "ProcessHost must report facts instead of owning {forbidden}"
        );
    }
}
