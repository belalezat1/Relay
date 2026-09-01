# Relay

Relay is a Java/Spring Boot workflow orchestration engine for durable, dependency-aware task execution.

The project is intentionally small but realistic: it models workflows, tasks, and task attempts in PostgreSQL, resolves dependency order, executes ready work, and exposes a REST API for submission and retrieval.

## Goals
- Durable workflow state backed by PostgreSQL
- Dependency-aware execution ordering
- Task attempt audit trail for retries and debugging
- Simple, testable orchestration model suitable for extension
- Local developer setup using Docker Compose and Maven

## Tech stack
- Java 21
- Maven multi-module build
- Spring Boot 3.4.x
- Spring Data JPA
- Flyway
- PostgreSQL

## Repository layout
- `api/` — REST API module
- `core/` — domain model, repositories, orchestration logic
- `docker-compose.yml` — local PostgreSQL environment
- `pom.xml` — parent Maven build configuration
- `docs/` — local working notes and design artifacts (gitignored)

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

## Architecture summary
- `core` owns the domain model and orchestration rules.
- `api` exposes HTTP endpoints and translates requests/responses.
- Postgres holds the durable workflow, task, and attempt state.
- Flyway applies schema changes consistently for each environment.

## Notes
- This repository keeps design and planning documents in a local `docs/` folder for personal working notes and does not treat them as source-controlled project artifacts.
- The project is intentionally structured to support later phases such as distributed workers, retries, and queue-backed execution without reworking the data model.

## Project roadmap
- Phase I: project bootstrap and stable Java + Maven setup
- Phase II: PostgreSQL-backed persistence and Flyway schema
- Phase III: workflow orchestration, dependency resolution, and REST API
- Phase IV: retry handling, worker execution loop, and dead-lettering
- Phase V: operational hardening, queue-backed dispatch, and production-oriented APIs

## Next phases
- Add richer workflow execution semantics and scheduling
- Add dead-lettering and retry policies
- Add worker polling and task dispatch
- Add more robust API validation and error mapping
