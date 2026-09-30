# Load-shedding allocation

This experimental standalone TA strategy keeps consistent-hashing homes and moves
targets only to relieve overloaded collectors. Run one TA replica; leader election
and Operator CRD configuration are deferred.

```yaml
allocation_strategy: load-shedding
allocation_strategy_config:
  load_shedding:
    interval: 5s
    window: 6
    default_size: 1000
    probe_interval: 30s
    probe_timeout: 5s
    probe_concurrency: 4
    probe_max_bytes: 67108864 # 64 MiB, after decompression
```

These are the defaults. New targets use `default_size` at the next allocation
cycle without waiting for a probe. Each cycle publishes one complete assignment
snapshot using current discovery, membership and estimates.

## Asynchronous sizing

TA probes targets itself. Workers select due targets from current discovery;
at most `probe_concurrency` requests run concurrently. Each target is retried
`probe_interval` after its previous attempt finishes, subject to worker availability.
Job configuration supplies authentication, TLS, scheme, path, parameters and
target relabeling. TA must have access to the job's credentials and certificate files.

Probes request Prometheus/OpenMetrics text and count non-empty, non-comment lines
in 32 KiB chunks. Gzip is supported. No metric names, labels or values are parsed
or retained. This estimates exposed sample count, not unique series after metric
relabeling or the cost of native histograms. Empty responses have weight one.

Timeouts, non-200 responses, unsupported formats, read errors and oversized bodies
keep the previous estimate, or the default if no probe has succeeded. Bodies are
not extrapolated when truncated. Increase `probe_max_bytes` for larger targets.
Results from removed targets or replaced job configurations are discarded.
Probing adds an extra HTTP scrape; it does not send metrics to remote write.

## Placement

Sizes use a six-cycle rolling maximum, followed by another maximum of their sum.
After a lower estimate arrives, the previous size can affect capacity for up to
eleven allocation cycles. Failed probes retain their last estimate indefinitely.

Collectors above 1.2× ideal load shed home targets in stable hash order. Moves
leave donors at or above 0.8× ideal and recipients at or below 1.1× ideal.
Overloaded home collectors never adopt. Targets that cannot move stay home.
The existing `buraksezer/consistent` library supplies homes and candidate order.
This does not guarantee a hard capacity bound or eliminate assignment churn.

Alloy continues using `/jobs/<job>/targets?collector_id=<id>` through
`discovery.http`. There is no size-report endpoint. Estimates and windows are
in memory and reset on restart. The `opentelemetry_allocator_overloaded_collectors`
gauge counts collectors still above capacity after shedding.
