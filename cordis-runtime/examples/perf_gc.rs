//! Public-path garbage collection complexity harness.

#![allow(missing_docs, clippy::cast_possible_truncation)]

use cordis_runtime::{Runtime, RuntimeConfig};
use std::{env, time::Instant};

fn number(name: &str, default: usize) -> usize {
    env::args()
        .skip(1)
        .find_map(|arg| arg.strip_prefix(&format!("--{name}=")).map(str::to_owned))
        .map_or(default, |value| value.parse().expect("numeric argument"))
}

fn median(values: &mut [u128]) -> u128 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), cordis_core::CordisError> {
    let total = number("total", 1_000);
    let reclaimable = number("reclaimable", 0).min(total);
    let rounds = number("rounds", 7).max(1);
    let mut config = RuntimeConfig::default();
    config.max_scopes = total + 1_024;
    config.max_fibers = total + 1_024;
    let runtime = Runtime::with_config(config)?;
    let mut scopes = Vec::with_capacity(total);
    for index in 0..total {
        scopes.push(runtime.create_scope(runtime.root(), format!("gc-{index}"))?);
    }
    for scope in scopes.iter().take(reclaimable) {
        runtime.dispose_scope(*scope).await?;
    }
    let before = runtime.snapshot();
    let mut times = Vec::with_capacity(rounds);
    let mut reclaimed_fibers = 0;
    let mut reclaimed_scopes = 0;
    for _ in 0..rounds {
        let start = Instant::now();
        let report = runtime.collect_garbage();
        times.push(start.elapsed().as_nanos());
        reclaimed_fibers += report.fibers;
        reclaimed_scopes += report.scopes;
    }
    let minimum = times.iter().copied().min().unwrap_or(0);
    let maximum = times.iter().copied().max().unwrap_or(0);
    let median = median(&mut times);
    println!(
        "CORDIS_GC_RESULT total={total} requested_reclaimable={reclaimable} visible_before={} rounds={rounds} min_ns={minimum} median_ns={median} max_ns={maximum} reclaimed_fibers={reclaimed_fibers} reclaimed_scopes={reclaimed_scopes}",
        before.scopes.len()
    );
    runtime.shutdown().await?;
    Ok(())
}
