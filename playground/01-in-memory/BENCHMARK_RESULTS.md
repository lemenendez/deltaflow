# 01 In-Memory Benchmark Results

Use this file to record repeatable throughput runs for v0.10.0 tuning.

## Run Command

```bash
go run . \
  -mode bench \
  -seed 42 \
  -universe 1000 \
  -mutations 100000 \
  -ghost-every 10 \
  -concurrency 1,2,4,8 \
  -batch 1,8,16,32
```

## Result Template

- Date:
- Machine:
- Go version:
- Seed:
- Universe:
- Mutations:
- Ghost every:
- Lock for:

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 |  |  | 1.00x |  |  |  |  |

## Recorded Run - 2026-06-19 (post claim-path optimization)

- Date: 2026-06-19
- Machine: leo@pop-os
- Go version: not captured
- Seed: 42
- Universe: 1000
- Mutations: 10000
- Ghost every: 10
- Lock for: default

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 180ms | 55709.14 | 1.00x | 10000 | 0 | 0 | 1000 |
| 1 | 8 | 106ms | 94006.23 | 1.69x | 10000 | 0 | 0 | 1000 |
| 1 | 16 | 92ms | 108167.91 | 1.94x | 10000 | 0 | 0 | 1000 |
| 1 | 32 | 75ms | 133379.48 | 2.39x | 10000 | 0 | 0 | 1000 |
| 2 | 1 | 151ms | 66229.76 | 1.19x | 10000 | 0 | 0 | 1000 |
| 2 | 8 | 95ms | 105472.22 | 1.89x | 10000 | 0 | 0 | 1000 |
| 2 | 16 | 86ms | 115882.13 | 2.08x | 10000 | 0 | 0 | 1000 |
| 2 | 32 | 98ms | 102248.36 | 1.84x | 10000 | 0 | 0 | 1000 |
| 4 | 1 | 167ms | 59830.33 | 1.07x | 10000 | 0 | 0 | 1000 |
| 4 | 8 | 91ms | 109627.93 | 1.97x | 10000 | 0 | 0 | 1000 |
| 4 | 16 | 92ms | 108615.13 | 1.95x | 10000 | 0 | 0 | 1000 |
| 4 | 32 | 79ms | 127214.67 | 2.28x | 10000 | 0 | 0 | 1000 |
| 8 | 1 | 170ms | 58760.64 | 1.05x | 10000 | 0 | 0 | 1000 |
| 8 | 8 | 90ms | 110686.90 | 1.99x | 10000 | 0 | 0 | 1000 |
| 8 | 16 | 88ms | 113990.48 | 2.05x | 10000 | 0 | 0 | 1000 |
| 8 | 32 | 64ms | 155779.30 | 2.80x | 10000 | 0 | 0 | 1000 |

### Quick Read

- Claim-path optimization removed the previous misleading profile.
- Concurrency now helps at batch=1 up to a point, then flattens.
- Batch still reduces claim overhead and improves throughput.
- Best observed case here: concurrency=8, batch=32.

Notes:
- Keep seed, universe, and mutations fixed across comparisons.
- Validate correctness counters while optimizing throughput.
