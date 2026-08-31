# Product Requirements Document: Relay

**A Distributed Task Orchestration Engine**

---

## 1. Summary

Relay is a service that accepts multi-step workflows (ordered or dependent tasks) and guarantees they run to completion exactly once, even when individual workers crash, tasks fail transiently, or the system scales across multiple nodes. It is a small-scale analog to tools like Temporal, Airflow, or Sidekiq, built to demonstrate the reliability mechanisms that make asynchronous backend systems trustworthy at scale.

## 2. Problem Statement

Any backend system that performs multi-step or background work (order processing, ETL, notification pipelines, scheduled jobs) eventually runs into the same set of failure modes:

- A worker crashes mid-task, and naive retries risk double-executing side effects (charging a card twice, sending a duplicate email).
- A single step in a multi-step process fails, and without orchestration the whole job is lost or requires bespoke recovery logic per service.
- Scaling to multiple worker instances introduces the risk of two workers claiming the same scheduled task simultaneously.
- Permanently failing tasks (bad input, a downstream dependency that is gone for good) silently disappear instead of surfacing for inspection.

Most teams solve this by hand-rolling retry and locking logic inside every individual service, which is exactly how systems become fragile and hard to maintain over time. Relay centralizes this logic once.

## 3. Goals

- Accept a workflow definition (a set of tasks with dependencies) via an API and execute it reliably.
- Guarantee at-least-once delivery with idempotent execution, so retries never double-apply side effects.
- Automatically retry transient failures with exponential backoff.
- Route permanently failed tasks to a dead-letter queue for inspection rather than losing them silently.
- Guarantee that scheduled or singleton tasks execute on exactly one worker even when multiple workers are running.
- Scale horizontally by adding worker instances without redesigning the system.

## 4. Non-Goals

- Relay is not a general-purpose message broker; it uses one (Kafka) internally but does not expose broker-level features to clients.
- No multi-tenant authentication/authorization system beyond a basic API key check.
- No cross-region or multi-datacenter failover in the initial version.
- No visual workflow-builder UI; workflows are defined via API/config, not drag-and-drop.

## 5. Target Use Case (Reference Scenario)

An order-processing pipeline: charge card → reserve inventory → send confirmation email. Each step can fail independently. Relay should guarantee the whole workflow completes correctly exactly once, retries the card charge safely if the payment service times out, and surfaces the workflow for manual review if inventory reservation fails permanently after retries.

## 6. Functional Requirements

| ID | Requirement |
|----|-------------|
| F1 | Client can submit a workflow (ordered/dependent tasks) via REST API |
| F2 | System persists workflow and task state durably (survives a full restart) |
| F3 | Tasks are distributed to worker nodes for execution |
| F4 | Failed tasks are retried automatically with exponential backoff, up to a configurable max attempts |
| F5 | Tasks that exceed max attempts are moved to a dead-letter queue and flagged for inspection |
| F6 | Task execution is idempotent: redelivery of an already-completed task does not re-execute it |
| F7 | Scheduled/singleton tasks acquire a distributed lock so exactly one worker executes them |
| F8 | Client can query workflow/task status via API |
| F9 | System exposes basic metrics (throughput, failure rate, retry counts) |

## 7. Non-Functional Requirements

- **Reliability**: no task should be silently lost under normal failure conditions (worker crash, transient network failure).
- **Scalability**: adding worker instances should linearly increase task throughput up to broker/database limits.
- **Observability**: every task's state transitions (queued → running → succeeded/failed/retrying → dead-lettered) must be visible.
- **Performance target**: sustain at least a few hundred tasks/sec on a modest local cluster (exact number to be measured, not assumed, during load testing).

## 8. Success Metrics

- Demonstrated throughput under load test (tasks/sec), with a documented number rather than an estimate.
- Zero duplicate side-effect executions across a chaos test that kills workers mid-task.
- 100% of permanently-failed tasks appear in the dead-letter queue, none silently disappear.
- Horizontal scaling test: throughput measurably increases when worker count increases (e.g., 1 → 3 workers).

## 9. Future Work (Out of Scope for v1)

- Web UI for workflow visualization and DLQ inspection.
- Cross-region replication.
- Workflow versioning/migration for in-flight workflows.
