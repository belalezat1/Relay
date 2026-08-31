# Task Execution Plan: Relay

4-6 week build plan, broken into weekly milestones with concrete deliverables. Weeks 5-6 are stretch scope if time allows; the project is resume-ready after Week 4.

---

## Week 1: Core API + Data Model

**Goal**: A single-worker version that can accept and run a workflow end to end, no distribution yet.

- [ ] Set up Spring Boot project (Web, Data JPA, Validation dependencies)
- [ ] Design Postgres schema:
  - `workflows` (id, status, created_at, updated_at)
  - `tasks` (id, workflow_id, type, payload, status, attempt_count, depends_on)
  - `task_attempts` (id, task_id, started_at, finished_at, result, error)
- [ ] REST endpoint: `POST /workflows` — accepts a workflow definition (list of tasks + dependencies), persists it
- [ ] REST endpoint: `GET /workflows/{id}` — returns workflow + task statuses
- [ ] Simple in-process executor that walks the dependency graph and runs tasks sequentially (no Kafka yet)
- [ ] Unit tests for the dependency-resolution logic (a task only runs once its dependencies succeed)

**Deliverable**: a workflow of 3+ dependent tasks submitted via API executes correctly single-threaded.

---

## Week 2: Distribution via Kafka

**Goal**: Tasks are distributed across multiple worker processes instead of running in-process.

- [ ] Stand up Kafka locally (Docker Compose)
- [ ] Producer: when a task becomes eligible to run (dependencies satisfied), publish it to a Kafka topic
- [ ] Consumer/worker: separate process that consumes tasks from Kafka and executes them
- [ ] Run 2+ worker instances simultaneously, confirm tasks are load-balanced across them (Kafka consumer group)
- [ ] On task completion, worker publishes a result event; the orchestrator consumes it and unlocks dependent tasks
- [ ] Retry logic: on task failure, republish with exponential backoff (delay topic or scheduled retry table)

**Deliverable**: a workflow executes correctly with 2-3 worker processes running independently, confirmed by logs showing which worker handled which task.

---

## Week 3: Idempotency + Dead-Letter Queue

**Goal**: The system survives worker crashes and permanent failures without data corruption or silent loss.

- [ ] Add idempotency key per task attempt; before executing, check if this exact attempt was already marked complete
- [ ] Chaos test: kill a worker process mid-task, confirm the task is picked up by another worker and does not double-execute its side effect (use a mock "external API call" with a counter to verify)
- [ ] Implement max-retry cutoff; after N failed attempts, move the task to a `dead_letter_tasks` table/topic instead of retrying indefinitely
- [ ] REST endpoint: `GET /dead-letters` — lists permanently failed tasks for inspection
- [ ] Test: a task designed to always fail ends up in the dead-letter queue exactly once, not lost and not retried forever

**Deliverable**: documented chaos test results (what was killed, what was expected, what actually happened) showing zero duplicate executions and zero silent task loss.

---

## Week 4: Distributed Locking + Containerization

**Goal**: Scheduled/singleton tasks are safe under concurrent workers, and the whole system is packaged for easy deployment.

- [ ] Implement distributed lock for singleton tasks using Postgres advisory locks (`pg_advisory_lock`)
- [ ] Test: run a "scheduled" task type with 5 concurrent workers polling simultaneously, confirm exactly one executes it per scheduled interval
- [ ] Dockerize: Dockerfile for the API service, Dockerfile for the worker, docker-compose.yml wiring Postgres + Kafka + API + N workers
- [ ] Write the README: architecture overview, how to run locally, how to submit a workflow via curl/Postman example
- [ ] Tag this as v1 — this is the resume-ready checkpoint even if Weeks 5-6 don't happen

**Deliverable**: `docker-compose up` brings up the full system; a scripted demo submits a sample order-processing workflow and shows it completing correctly.

---

## Week 5 (stretch): Observability

- [ ] Add basic metrics (Micrometer + Prometheus): tasks processed/sec, failure rate, retry count, dead-letter count
- [ ] Grafana dashboard visualizing the above
- [ ] Load test (e.g., k6 or a simple script) submitting hundreds/thousands of workflows, recording actual throughput numbers

**Deliverable**: a real throughput number (not an estimate) and a dashboard screenshot for the README.

---

## Week 6 (stretch): Scaling Proof + Polish

- [ ] Minimal Kubernetes manifests (Deployment + Service for API and workers, StatefulSet or managed service for Postgres/Kafka)
- [ ] Run the same load test at 1 worker replica vs. 3-5 replicas, document the throughput difference
- [ ] Record a short demo (video or GIF) of the chaos test and the scaling test for the portfolio/README
- [ ] Final pass on code cleanliness, test coverage, and documentation

**Deliverable**: documented evidence that horizontal scaling actually increases throughput, plus a polished repo ready to link on the resume.

---

## Notes on Sequencing

- Weeks 1-2 are the highest-risk, highest-learning weeks (Kafka integration, distributed execution). Don't skip ahead to locking/observability before this works correctly.
- The chaos test in Week 3 is the single most valuable piece of evidence for interviews — it proves the reliability claims rather than just asserting them. Prioritize it over polish.
- If time runs short, stop after Week 4. A working, dockerized, chaos-tested system is a complete and defensible project on its own.
