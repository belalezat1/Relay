#!/usr/bin/env bash
# Relay resume-metrics benchmark harness.
# Measures Kafka consumer-group throughput for 1 worker vs 3 workers on a fixed 2-node DAG
# and regenerates docs/benchmarks.md with measured numbers (never invented).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"
if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  JAVA_HOME="/opt/homebrew/opt/openjdk@21"
fi
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"

RESULTS_MD="${RESULTS_MD:-$ROOT/docs/benchmarks.md}"
WARMUP_DAGS="${WARMUP_DAGS:-40}"
MEASURED_DAGS="${MEASURED_DAGS:-600}"

echo "== Relay benchmarks =="
echo "JAVA_HOME=$JAVA_HOME"
echo "RESULTS_MD=$RESULTS_MD"
echo "WARMUP_DAGS=$WARMUP_DAGS MEASURED_DAGS=$MEASURED_DAGS"

export RELAY_BENCH=true
export RELAY_BENCH_OUTPUT="$RESULTS_MD"
export WARMUP_DAGS
export MEASURED_DAGS

RELAY_BENCH=true mvn -pl api -am -Dtest=ResumeThroughputBenchmarkIT \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test

echo "Wrote $RESULTS_MD"
