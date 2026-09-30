# Load-shedding allocation

This experimental standalone TA strategy keeps consistent-hashing homes and moves
targets only to relieve overloaded collectors. Run one TA replica. Operator CRD
configuration and leader election are not implemented.

```yaml
allocation_strategy: load-shedding
allocation_strategy_config:
  load_shedding:
    interval: 5s
    window: 6
    default_size: 1000
```

These are the defaults. `interval` must be at least 1ms, `window` must be 1–120,
and `default_size` must be positive and at most 1e12. Use the same units for all
estimates, such as exposed sample count.

Each cycle uses the latest discovered targets, collectors and reported sizes.
New targets receive the default estimate and become available at the next cycle,
without waiting for a size report. Intermediate discovery updates are coalesced.
Publication replaces the whole assignment snapshot; HTTP readers retain a
consistent view while the next assignment is computed.

The strategy takes a rolling maximum of each target's size over `window` cycles,
then another rolling maximum of their sum. With six cycles, a measurement can
affect capacity for eleven cycles (55 seconds at the default interval).

Collectors above 1.2× ideal load shed home targets in stable hash order. A move
must leave its donor at or above 0.8× ideal and its recipient at or below 1.1×
ideal. Collectors overloaded by their home targets never adopt. If a target
cannot be moved, it stays home. This does not guarantee a hard capacity bound.
The existing `buraksezer/consistent` library provides homes and candidate order;
`GetClosestN` follows physical-member hashes after the home, not virtual-ring
successors. Targets sharing a URL are counted separately by their job/label identity.

## Size reports

TA does not measure targets itself. A reporter can list target identities using
`GET /target-sizes`; each entry contains `id`, `job`, `url` and `labels`. IDs are
decimal strings, avoiding loss of precision in JSON clients.

Submit a partial batch with `POST /target-sizes`:

```sh
curl -X POST http://target-allocator:80/target-sizes \
  -H 'Content-Type: application/json' \
  -d '{"1234567890123456789":2000}'
```

Replace the example ID with one from GET. Sizes must be finite, positive and at
most 1e12. Bodies are limited to 1 MiB. Invalid batches return 400 without updating
any estimates. Valid batches return 202 with `accepted` and `ignored` counts;
unknown or removed targets are ignored. Updates take effect at the next cycle.
The latest estimate is reused until another report arrives or the target disappears.
Estimates and windows are in memory and are lost on restart.

These routes exist only for load-shedding and use the existing TA HTTP/HTTPS
listeners. The HTTP listener has no authentication; keep report access restricted
to trusted clients, or use the existing mTLS listener.

Alloy continues to use `/jobs/<job>/targets?collector_id=<id>` through
`discovery.http`. Size-reporting clients do not need changes to that response.
The `opentelemetry_allocator_overloaded_collectors` gauge counts collectors still
above capacity after shedding. Target counts and allocation duration remain available.
