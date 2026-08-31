# Phase II Plan: Persistence Layer and Domain Model

## Problem and approach

Phase I created the project skeleton, Maven multi-module layout, Spring Boot app bootstrap, and Docker/Postgres local environment. Phase II is focused on making the system real by moving from empty project scaffolding to a durable persisted workflow model.

The implementation will use the existing Maven parent module, core/api split, Docker Compose Postgres service, and Spring Boot app configuration as the base. We will add Flyway migrations for the relational schema, JPA entities for workflows/tasks/task attempts, repositories for persistence, and a small validation path that proves the application can connect to Postgres and run the schema successfully.

## Scope

In scope for Phase II:
- Postgres schema for workflows, tasks, and task_attempts
- Flyway versioned migration that creates schema and indexes
- JPA entities and enums mapped to the DB
- Data repositories and repository queries for fetching workflows/tasks
- Validation that app starts successfully against the Dockerized Postgres instance
- Small smoke tests for database connectivity and schema correctness

Out of scope for Phase II:
- Kafka distribution, retries, idempotency, DLQ, or worker execution
- REST endpoints for workflow submission and status retrieval
- Dependency-resolution/execution orchestration logic

## Files likely to change

- [pom.xml](/Users/b/Projects/Relay/pom.xml)
- [api/pom.xml](/Users/b/Projects/Relay/api/pom.xml)
- [api/src/main/resources/application.properties](/Users/b/Projects/Relay/api/src/main/resources/application.properties)
- [api/src/main/java/com/relay/api/RelayApplication.java](/Users/b/Projects/Relay/api/src/main/java/com/relay/api/RelayApplication.java)
- [core/pom.xml](/Users/b/Projects/Relay/core/pom.xml)
- New migration under [api/src/main/resources/db/migration/](/Users/b/Projects/Relay/api/src/main/resources)
- New JPA package under [core/src/main/java/com/relay/core/](/Users/b/Projects/Relay/core/src/main/java/com/relay/core)
- New repository package under [core/src/main/java/com/relay/core/repository/](/Users/b/Projects/Relay/core/src/main/java/com/relay/core)

## Implementation plan

### 1. Schema and migration design
- Confirm final schema shape based on the design doc: `workflows`, `tasks`, `task_attempts`
- Add Flyway migration `V1__Initial_Schema.sql`
- Keep the schema intentionally simple and durable, with rich status enums and indexes
- Use Postgres enums or varchar-backed enums if we want the simplest stable JPA mapping

### 2. Domain model
- Add `Workflow`, `Task`, and `TaskAttempt` entities in core module
- Add enums for workflow/task status and task attempt result
- Define JPA relationships and basic audit fields (`createdAt`, `updatedAt`, `startedAt`, `finishedAt`)
- Keep JSON payload flexible with `@Column(columnDefinition = "jsonb")` or equivalent mapping for Postgres

### 3. Repositories and query shape
- `WorkflowRepository` with basic CRUD and lookup by ID
- `TaskRepository` with query methods for workflow-scoped tasks and status filtering
- `TaskAttemptRepository` for task-attempt history
- Add the query methods needed for Week 1 orchestration later without overengineering the interface now

### 4. Spring wiring and validation
- Ensure the app boots and connects to the local Postgres container started by Docker Compose
- Run Flyway migration successfully during startup
- Validate that JPA can persist at least one entity shape in a smoke test
- Keep this phase focused on persistence correctness, not orchestration logic

### 5. Testing and docs
- Add a small repository-layer or integration test to assert persistence works against local Postgres
- Document the schema and migration approach in the project docs under the `docs/` folder
- Use Phase I docs as baseline and update current design references to reflect Phase II completion

## Notes and considerations

- We should keep the schema aligned with the design document and avoid premature enterprise abstractions. This is a small implementation in a greenfield repo; the goal is a working durable model, not a generalized framework.
- The Postgres container already exists in Docker Compose, so we should use it immediately rather than creating alternative dev-only DB setup.
- We need to avoid introducing Kafka or workflow execution machinery in this phase; doing so would blur responsibilities and increase risk.
- For the initial version, status enums and simple foreign keys are sufficient. We can extend later when Week 3+ adds retrying and dead-letter behavior.
- The migration should be safe to run against a fresh `relay_dev` database and should clearly support future schema upgrades via Flyway.

## Todo list

1. Create the initial Flyway migration for workflow/task/task_attempt tables and enums.
2. Define the JPA domain model and enums for workflow/task/task attempts.
3. Add repository interfaces and core query methods.
4. Validate database connectivity and migration execution via Spring Boot startup.
5. Add a focused integration smoke test for persistence.
6. Document the schema decision and migration notes in docs.
