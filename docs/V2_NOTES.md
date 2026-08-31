# Post-v1 candidates

These items do not block the Kernel design version 1.0 freeze:

- compute/blocking pools, CPU affinity, NUMA, and custom scheduling;
- Agent, model, tool, planner, memory, and LLM layers;
- dynamic plugin ABI and distributed runtime support;
- more sophisticated interner reclamation or generation-safe symbol reuse;
- adaptive cache eviction and generational garbage collection;
- richer shutdown progress streaming beyond durable per-attempt outcomes.

The next phase is **V2 Performance Characterization**: benchmark and profile the frozen v1 contract before proposing architectural optimization. No executor, compute-pool, lock-free, affinity, or GC/interner redesign is justified without measured evidence.
