# Relay benchmarks

Phase IX Definition of Done includes a local throughput harness.

## How to run

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh
```

The script starts Compose (`--profile app`) when Docker is available, submits `TASK_COUNT` (default 30) workflows for 1-worker and concurrency=3 pressure, and performs a duplicate idempotency-key check.

## Results

| Scenario | Workers | Tasks | Duration | Throughput | Source |
| --- | ---: | ---: | --- | --- | --- |
| in-process 1-worker | 1 | 30 | — | — | run locally |
| in-process concurrency=3 | 3 | 30 | — | — | run locally |
| crash-injection duplicate check | n/a | 1 | — | — | run locally |

Docker Desktop was not reachable in the environment that last regenerated this placeholder table. Re-run `./scripts/benchmark.sh` on a machine with Docker to fill measured values.

## Related automated evidence (always available via `mvn test`)

- `KafkaReliabilityTest.concurrentConsumersOnlyExecuteOnce` / `parallelClaimServiceDoesNotDoubleExecute`
- `TaskClaimServicePostgresIT.skipLockedAllowsOnlyOneClaimer` (when Docker available)
- `PhaseIXIntegrationTest` outbox + dual-claimer coverage (when Docker available)
