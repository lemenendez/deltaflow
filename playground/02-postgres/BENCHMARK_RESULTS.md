# 02 Postgres Benchmark Results

Use this file to record repeatable throughput runs for v0.10.0 tuning.

## Run Command

```bash
make up
make migrate
DELTAFLOW_PG_DSN='postgres://deltaflow:deltaflow@localhost:5432/deltaflow?sslmode=disable' \
  go run . \
  -mode bench \
  -seed 42 \
  -universe 1000 \
  -mutations 50000 \
  -ghost-every 10 \
  -concurrency 1,2,4,8 \
  -batch 1,8,16,32
```

## Result Template

- Date:
- Machine:
- Go version:
- Postgres version:
- Seed:
- Universe:
- Mutations:
- Ghost every:
- Lock for:

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Worker runs | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 |  |  | 1.00x |  |  |  |  |  |

## Recorded Run - 2026-06-19

- Date: 2026-06-19
- Machine: leo@pop-os
- Go version: not captured
- Postgres version: postgres:17 (docker image)
- Seed: 42
- Universe: 500
- Mutations: 2000
- Ghost every: 10
- Lock for: 30s

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Worker runs | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 12.249s | 163.28 | 1.00x | 2000 | 2000 | 0 | 0 | 200 |
| 1 | 8 | 4.396s | 455.01 | 2.79x | 256 | 2000 | 0 | 0 | 200 |
| 2 | 1 | 7.435s | 268.98 | 1.65x | 1008 | 2000 | 0 | 0 | 200 |
| 2 | 8 | 3.263s | 612.95 | 3.75x | 128 | 2000 | 0 | 0 | 200 |
| 4 | 1 | 15.400s | 129.87 | 0.80x | 512 | 2000 | 0 | 0 | 198 |
| 4 | 8 | 3.920s | 510.20 | 3.12x | 64 | 2000 | 0 | 0 | 200 |

### Quick Read

- Postgres shows a different curve than in-memory: moderate concurrency with batching performs best in this run.
- Best observed case here: concurrency=2, batch=8.
- Very high concurrency with tiny batch can regress due to contention/overhead.
- Ghost count mismatch in one case (`c=4,b=1`) suggests a benchmark accounting race and should be validated before final default selection.

## Recorded Run - 2026-06-19 (terminal sample #2)

- Date: 2026-06-19
- Machine: leo@pop-os
- Go version: not captured
- Postgres version: postgres:17 (docker image)
- Seed: 42
- Universe: 500
- Mutations: 2000
- Ghost every: 10
- Lock for: 30s

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Worker runs | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 15.076s | 132.66 | 1.00x | 2000 | 2000 | 0 | 0 | 200 |
| 1 | 8 | 5.294s | 377.78 | 2.85x | 250 | 2000 | 0 | 0 | 200 |
| 2 | 1 | 11.014s | 181.59 | 1.37x | 1000 | 2000 | 0 | 0 | 200 |
| 2 | 8 | 4.290s | 466.18 | 3.51x | 125 | 2000 | 0 | 0 | 200 |
| 4 | 1 | 15.747s | 127.01 | 0.96x | 500 | 2000 | 0 | 0 | 199 |
| 4 | 8 | 4.157s | 481.16 | 3.63x | 63 | 2000 | 0 | 0 | 200 |

### Quick Read

- Ranking is consistent with sample #1: batching dominates and `c=2,b=8`/`c=4,b=8` are best in Postgres.
- Single-job mode (`batch=1`) does not benefit from higher concurrency in this profile.
- Ghost count mismatch appears again for `c=4,b=1` (199), so benchmark ghost accounting should be treated as approximate until fixed.

## Recorded Run - 2026-06-19 (larger universe variation)

- Date: 2026-06-19
- Machine: leo@pop-os
- Go version: not captured
- Postgres version: postgres:17 (docker image)
- Seed: 42
- Universe: 10000
- Mutations: 2000
- Ghost every: 10
- Lock for: 30s
- Variation: fixed `batch=8` across `concurrency=1,2,4,8` plus baseline `1x1`

| Concurrency | Batch | Duration | Jobs/sec | Speedup vs 1x1 | Worker runs | Synced | Dead | Retrying | Ghosts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 9.216s | 217.02 | 1.00x | 2000 | 2000 | 0 | 0 | 200 |
| 1 | 8 | 3.910s | 511.54 | 2.36x | 256 | 2000 | 0 | 0 | 200 |
| 2 | 8 | 3.038s | 658.43 | 3.03x | 128 | 2000 | 0 | 0 | 200 |
| 4 | 8 | 2.886s | 693.05 | 3.19x | 64 | 2000 | 0 | 0 | 200 |
| 8 | 8 | 5.290s | 378.07 | 1.74x | 128 | 2000 | 0 | 0 | 199 |

### Quick Read

- Increasing universe improved concurrency scaling up to `c=4` for this fixed-batch variation.
- Best observed point in this variation: `c=4,b=8`.
- `c=8` regressed, indicating contention/overhead beyond the useful concurrency window on this laptop setup.
- Ghost count mismatch persists at high concurrency (`c=8,b=8`), so ghost counter should be treated as advisory until benchmark accounting is hardened.

Notes:
- Keep seed, universe, and mutations fixed across comparisons.
- Compare Postgres results against 01 in-memory to estimate storage/locking overhead.
