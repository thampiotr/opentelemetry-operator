# Sticky allocation

Experimental standalone TA strategy. Run one TA replica: ownership is local to
that process, and independent replicas can disagree after different histories.
Leader election and assignment replication are not implemented.

```yaml
allocation_strategy: sticky
allocation_strategy_config:
  sticky:
    interval: 5s
    window: 6
    default_size: 1000
    probe_interval: 30s
    probe_timeout: 5s
    probe_concurrency: 4
    probe_max_bytes: 67108864
```

These are the defaults. [Asynchronous probes and size windows](load-shedding.md#asynchronous-sizing)
are shared with load-shedding. New targets use a default estimate without waiting
for a probe. Each allocation cycle uses current discovery and publishes a complete
snapshot; Alloy continues consuming the existing HTTP discovery endpoint.

Existing targets retain their owner. New targets and targets whose owner left
are placed largest-first on the least-loaded collector. Ties use stable target
hashes and collector names. A target always has an owner when collectors exist;
a departed collector's targets are reassigned in the next cycle without delay.

A collector above 1.2× ideal load for `window` consecutive cycles is rebalanced.
Process donors heaviest-first and move their largest-fitting targets onto the
least-loaded collector, keeping the receiver at or below 1.1× ideal and reducing
the pair's peak load. Stop when the donor is below 1.2× ideal. There are no swaps
or hard balance guarantees. A joining collector receives new targets immediately;
redistributing established targets waits for the overload condition.

Assignments survive measurement-window expiry but reset on process restart.
Replicating assignments will be necessary for predictable HA handover. Removed
targets and collectors are pruned from placement state. The allocation explorer
at `/debug/allocation` shows published owners and estimated loads. The
`opentelemetry_allocator_overloaded_collectors` metric includes collectors waiting
for the overload delay and overload that cannot be repaired with permitted moves.

The allocator exports `opentelemetry_allocator_estimated_series_per_collector`
for its windowed load estimates and `opentelemetry_allocator_target_moves_total`
for owner changes of retained targets. The counter excludes initial placement,
deleted targets and transitions involving an unassigned target. It resets on
allocator restart, so it does not measure cold-start or failover reassignment.
