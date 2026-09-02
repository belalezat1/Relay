# Relay

Relay is a Java/Spring Boot workflow orchestration engine for durable, dependency-aware task execution.

The project is intentionally small but realistic: it models workflows, tasks, and task attempts in PostgreSQL, resolves dependency order, executes ready work, and exposes a REST API for submission and retrieval.

## Goals
- Durable workflow state backed by PostgreSQL
- Dependency-aware execution ordering
- Task attempt audit trail for retries and debugging
- Governance-aware workflow metadata and ownership boundaries
- Operator-friendly diagnostics, analytics, and lifecycle controls
- A clean event abstraction that can support Kafka later without changing the core execution model
- Local developer setup using Docker Compose and Maven

## Tech stack
- Java 21
- Maven multi-module build
- Spring Boot 3.4.x
- Spring Data JPA
- Flyway
- PostgreSQL
- Optional Kafka event transport behind an abstraction layer

## Repository layout
- `api/` — REST API module, lifecycle endpoints, audit access, and operator visibility
- `core/` — domain model, repositories, orchestration logic, audit tracking, and event abstraction
- `docker-compose.yml` — local PostgreSQL environment and optional app stack
- `pom.xml` — parent Maven build configuration
- `docs/` — design notes, runbooks, and milestone records

## Local setup
Prerequisites:
- Java 21
- Maven 3.9+
- Docker Desktop or Docker Engine

1. Copy the environment template if needed:
   ```bash
   cp .env.example .env
   ```

2. Start PostgreSQL:
   ```bash
   docker compose up -d postgres
   ```

3. Run the test suite:
   ```bash
   mvn test
   ```

4. Start the API with the default development profile:
   ```bash
   APP_ENV=dev DB_HOST=localhost DB_PORT=5432 DB_NAME=relay_dev DB_USERNAME=relay DB_PASSWORD=relay_dev APP_PORT=8080 mvn -pl api spring-boot:run
   ```

5. Or start the app through Docker Compose:
   ```bash
   docker compose --profile app up -d --build
   ```

6. Submit a workflow through the API:
   ```bash
   curl -X POST http://localhost:8080/api/workflows \
     -H "Content-Type: application/json" \
     -d '{
       "tasks": [
         {"id": "task-a", "type": "success"},
         {"id": "task-b", "type": "success", "dependsOn": ["task-a"]}
       ]
     }'
   ```

## Environment and deployment readiness
Relay now supports environment-specific configuration profiles and deployable container settings without changing the core runtime architecture.

- `dev` profile: local development defaults with debug logging and local database configuration.
- `prod` profile: production-oriented defaults with more conservative logging.
- `docker-compose.yml`: runs PostgreSQL and optionally the API in a local containerized setup.
- `.env.example`: centralizes deployment variables for the runtime and database.

Key runtime variables:
- `APP_ENV` — selects the active Spring profile (`dev` or `prod`)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD` — database connection settings
- `APP_PORT` — HTTP port for the API
- `WORKER_MAX_CONCURRENCY`, `WORKER_POLL_DELAY` — worker scheduling controls
- `relay.kafka.enabled` — turns on the optional Kafka-backed workflow event publisher
- `relay.kafka.topic` — the Kafka topic used for workflow lifecycle events

## Pre-Kafka platform maturity gate
The project intentionally stops before Kafka until the system is proven durable, governable, and operationally understandable.

Completed pre-Kafka work includes:
- explicit workflow ownership and environment metadata
- template and workflow-level governance defaults
- operator-friendly filters and workflow summaries
- durable audit logging and event abstraction
- optional Kafka transport behind the event publisher interface without making Kafka the workflow source of truth

This keeps Postgres as the authoritative workflow state while Kafka remains a later distributed transport option.

## Architecture summary
- `core` owns the domain model and orchestration rules.
- `api` exposes HTTP endpoints and translates requests/responses.
- Postgres holds the durable workflow, task, and attempt state.
- Flyway applies schema changes consistently for each environment.
- `WorkflowEventPublisher` decouples emitted events from the database-backed workflow engine so Kafka can be plugged in later.

## Notes
- Design and planning documents live in the `docs/` folder as part of the project’s working history and milestone records.
- The architecture is intentionally structured to add distributed messaging only after the single-node workflow engine is mature and operationally trusted.

## Project roadmap
- Phase I: project bootstrap and stable Java + Maven setup
- Phase II: PostgreSQL-backed persistence and Flyway schema
- Phase III: workflow orchestration, dependency resolution, and REST API
- Phase IV: retry handling, worker execution loop, and dead-lettering
- Phase V: operational hardening and runtime safety
- Phase VI: deployment and environment readiness
- Phase VII: platform maturity and integration boundaries
- Phase VIII: pre-Kafka readiness gate (governance, analytics, event abstraction, operator UX)
- Phase IX: Kafka adoption as the first distributed transport layer

## Next phase
Kafka is intentionally the next explicit distributed phase, not a shortcut for basic workflow maturity.
