# Design Document: Relay

**System design, architecture, and tooling rationale for a distributed task orchestration engine**

---

## 1. Architecture Overview

```
                    ┌─────────────────┐
   Client  ───POST──▶   API Service   │
                    │  (Spring Boot)  │
                    └────────┬────────┘
                             │ persists workflow/tasks
                             ▼
                    ┌─────────────────┐
                    │    PostgreSQL   │◀──────────────┐
                    │ (durable state) │                │
                    └────────┬────────┘                │
                             │ eligible tasks           │ status
                             ▼                          │ updates
                    ┌─────────────────┐                 │
                    │      Kafka      │                 │
                    │  (task topics)  │                 │
                    └────────┬────────┘                 │
                             │ consumed by               │
                 ┌───────────┼───────────┐              │
                 ▼           ▼           ▼              │
            ┌────────┐  ┌────────┐  ┌────────┐          │
            │Worker 1│  │Worker 2│  │Worker N│──────────┘
            └────────┘  └────────┘  └────────┘
```

The API service owns workflow submission and status queries. Postgres is the single source of truth for durable state (a task's status is never "only in Kafka"). Kafka is purely a distribution mechanism, not a system of record. Workers are stateless and horizontally scalable; any worker can pick up any task.

## 2. Data Model

**workflows**
| column | type | notes |
|---|---|---|
| id | UUID | primary key |
| status | enum | PENDING, RUNNING, COMPLETED, FAILED |
| created_at / updated_at | timestamp | |

**tasks**
| column | type | notes |
|---|---|---|
| id | UUID | primary key |
| workflow_id | UUID | foreign key |
| type | string | maps to a handler implementation |
| payload | jsonb | task-specific input |
| status | enum | PENDING, QUEUED, RUNNING, SUCCEEDED, FAILED, DEAD_LETTERED |
| depends_on | UUID[] | task IDs that must succeed first |
| attempt_count | int | current retry count |
| idempotency_key | string | unique per logical task attempt, deduplicates re-delivery |

**task_attempts**
| column | type | notes |
|---|---|---|
| id | UUID | primary key |
| task_id | UUID | foreign key |
| started_at / finished_at | timestamp | |
| result | enum | SUCCESS, FAILURE |
| error | text | nullable |

**dead_letter_tasks**
| column | type | notes |
|---|---|---|
| task_id | UUID | reference to the exhausted task |
| final_error | text | |
| moved_at | timestamp | |

## 3. Core Design Decisions

### 3.1 Idempotency

Every task attempt carries an idempotency key. Before a worker executes a task, it checks whether an attempt with that key already recorded a `SUCCESS` result. If so, it skips execution and simply acknowledges the message. This is what makes at-least-once delivery (Kafka's default guarantee) safe: redelivery after a crash does not re-run the side effect, it just confirms the already-done work.

### 3.2 Retry with Exponential Backoff

On failure, a task's `attempt_count` increments and it is republished with a delay proportional to `base_delay * 2^attempt_count` (capped at a max delay). This absorbs transient failures (a downstream API timing out) without hammering the failing dependency or requiring a human to intervene.

### 3.3 Dead-Letter Queue

Once `attempt_count` exceeds a configured max, the task moves to `dead_letter_tasks` instead of retrying again. This is a deliberate design choice: infinite retries on a permanently broken task (bad input, a dependency that no longer exists) waste resources and hide the failure. The DLQ makes failure visible and actionable instead of silent.

### 3.4 Distributed Locking for Singleton Tasks

Some tasks (scheduled/periodic jobs) must run on exactly one worker even when many workers are polling concurrently. Relay uses Postgres advisory locks (`pg_try_advisory_lock`) keyed on the task's schedule ID: a worker attempts to acquire the lock before executing; if it fails, another worker already has it, so this worker skips it. Postgres advisory locks were chosen over a dedicated coordination service (Zookeeper, etcd) because Postgres is already a hard dependency for durable state, so this avoids adding another moving part for a problem it already solves well at this scale.

### 3.5 Why Kafka Over a Simpler Queue (e.g. RabbitMQ, SQS)

Kafka was chosen specifically because consumer groups provide natural load balancing across worker instances without extra coordination code, and because partitioning gives a clear, well-understood path to scaling throughput (add partitions, add consumers). A simpler queue would work for the base case, but Kafka's partition model is the more defensible choice to explain in an interview when the explicit goal is demonstrating horizontal scalability.

## 4. Tooling Rationale

| Tool | Why chosen | Alternative considered |
|---|---|---|
| Spring Boot | Mature JVM framework, direct relevance to enterprise Java shops, strong ecosystem for REST + JPA | Plain Java + Javalin (more manual wiring, less resume-relevant) |
| PostgreSQL | ACID guarantees for durable workflow state, advisory locks solve distributed locking without new infra | MongoDB (weaker consistency guarantees for this use case) |
| Kafka | Consumer-group load balancing, partition-based scaling story | RabbitMQ (simpler, but weaker scaling narrative) |
| Docker / Docker Compose | Reproducible local environment matching how this would actually be deployed | Running services natively (harder to demo, less realistic) |
| Kubernetes (stretch) | Demonstrates deployment/scaling knowledge beyond a single-machine demo | Skip if time-constrained; Docker Compose alone is defensible |
| Prometheus/Grafana (stretch) | Turns "it's fast" into an actual measured number | Manual logging (works, but not visualized) |

## 5. Failure Scenarios and How the System Handles Them

| Scenario | Behavior |
|---|---|
| Worker crashes mid-task | Kafka redelivers to another consumer in the group; idempotency key prevents duplicate side effects |
| Downstream dependency times out | Task fails, retried with exponential backoff |
| Task fails permanently (bad input) | Exhausts retries, moves to dead-letter queue, visible via API |
| Two workers race for a scheduled task | Postgres advisory lock ensures only one proceeds |
| Worker pool scaled from 1 to 5 | Kafka rebalances partitions across the new consumer group members automatically |

## 6. Known Limitations (v1)

- Single-region, single Postgres instance: no cross-region failover story.
- No workflow versioning: an in-flight workflow assumes its definition doesn't change mid-execution.
- Dead-letter recovery is manual (an operator inspects and decides whether to requeue); no automatic remediation.

These are intentionally out of scope for the initial build and are worth naming explicitly in an interview as "things I'd tackle next," which demonstrates awareness of the problem's full scope rather than treating v1 as complete.
