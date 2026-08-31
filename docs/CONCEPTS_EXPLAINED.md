# Quick Reference: Gradle, Spring Boot, and Schema

---

## 1. Gradle: Build Tool

### What is it?
Gradle is a **build automation tool** — it compiles your Java code, runs tests, downloads dependencies, and packages everything into a runnable application. Think of it as the "orchestrator" for your development workflow.

**Similar tools**: Maven (older, XML-based), npm (JavaScript equivalent)

### Why does it matter?
- **Dependency management**: Gradle fetches required libraries (like Spring Boot, PostgreSQL driver) automatically
- **Task automation**: Build, test, run, package with single commands (`gradle build`, `gradle bootRun`)
- **Reproducibility**: Everyone on the team uses the same build process; no "works on my machine" surprises

### Latest version & Compatibility?
**Use latest: Gradle 8.x** (current stable). It's backwards compatible and works perfectly with Spring Boot 3.2.x. No compatibility concerns for this project.

**Why I ask about versions**: Legacy projects sometimes need older Gradle (e.g., for Java 8 compatibility), but this is a greenfield project, so latest is best. Same reasoning as using Python 3.12 for a new Django project instead of Python 2.7.

---

## 2. Spring Boot: Framework

### What is it?
Spring Boot is an **opinionated Java web framework** that makes building REST APIs and microservices fast and simple. It wraps the Spring Framework (a broader ecosystem) and gives you sensible defaults out of the box.

**Yes, it's similar to Django for Python** — both are:
- Frameworks that handle HTTP routing, database access, request/response handling
- Convention-over-configuration (they assume you'll do things "the right way")
- Great for REST APIs and web services

**Key difference**: Spring Boot is lower-level (closer to metal), so you have more explicit control; Django is higher-level and more batteries-included.

### How does it work?
1. **Annotations** drive everything:
   ```java
   @RestController
   @RequestMapping("/workflows")
   public class WorkflowController {
     @PostMapping
     public WorkflowResponse submitWorkflow(...) { ... }
   }
   ```
   This automatically wires up an HTTP endpoint: `POST /workflows`

2. **Dependency Injection**: Spring automatically creates objects and wires them together:
   ```java
   @Service
   public class WorkflowOrchestrator {
     private final TaskRepository repo;  // Spring injects this
     public WorkflowOrchestrator(TaskRepository repo) { 
       this.repo = repo; 
     }
   }
   ```

3. **Auto-configuration**: You tell Spring "I want a REST API + database", and it handles Tomcat (web server), connection pooling, serialization, etc.

### Why does it matter?
- **Speed**: You go from zero to a working REST API in hours, not days
- **Ecosystem**: Spring has libraries for everything (security, messaging, data, caching)
- **Industry standard**: Every Java shop uses Spring; learning it is a massive resume boost
- **Reduces boilerplate**: Without Spring, you'd manually wire HTTP handlers, database connections, and serialization

### Django Comparison

| Aspect | Spring Boot | Django |
|--------|-------------|--------|
| Language | Java | Python |
| Verbosity | More explicit, more boilerplate | Concise, lots of magic |
| Performance | Fast (compiled, JVM) | Slower (interpreted) |
| Learning curve | Steeper (Java + Spring idioms) | Gentler |
| Job market | Very high demand | Good demand |

---

## 3. Schema Explained

### The Big Picture
Relay stores **workflows** and their **tasks** in PostgreSQL. A workflow is a container; tasks are the individual work units inside it.

```
One Workflow
    ├─ Task A (charge card)
    ├─ Task B (reserve inventory) — depends on A succeeding
    └─ Task C (send email) — depends on B succeeding
```

### The Three Tables (Simplified View)

#### **workflows** — The Container
| Column | Meaning |
|--------|---------|
| `id` (UUID) | Unique workflow identifier |
| `status` | Current state: PENDING → RUNNING → COMPLETED or FAILED |
| `created_at` | When workflow was submitted |
| `updated_at` | Last time anything changed |

**Why this matters**: Clients query this to ask "Is my workflow done yet?"

#### **tasks** — The Work Units ⭐ (Most Important)
| Column | Meaning |
|--------|---------|
| `id` (UUID) | Unique task identifier |
| `workflow_id` | Which workflow owns this task (foreign key) |
| `type` | What kind of work: "charge_card", "send_email", etc. |
| `payload` (JSONB) | Input data: `{"amount": 100, "cardToken": "abc123"}` |
| `status` | Current state: PENDING → RUNNING → SUCCEEDED or FAILED |
| `depends_on` (UUID[]) | **⭐ CRITICAL**: Array of task IDs that must succeed first. E.g., `[task-A-id, task-B-id]` means "wait for A and B" |
| `attempt_count` | How many times we've tried to run this (for retries) |

**Why `depends_on` is the magic**: This single column is how Relay knows task execution order. The dependency resolver reads it and says "Task C can't run yet, Task A hasn't succeeded." Week 2 generalizes this to Kafka; Week 3 adds retries.

#### **task_attempts** — The Audit Trail
| Column | Meaning |
|--------|---------|
| `id` (UUID) | Unique attempt identifier |
| `task_id` | Which task was this an attempt for? |
| `started_at` | When execution began |
| `finished_at` | When execution ended |
| `result` | SUCCESS or FAILURE |
| `error` | Error message if FAILURE |

**Why this matters**: Audit trail. If a task failed 3 times before succeeding, we have a record of each attempt. Invaluable for debugging "why did this take so long?"

### Visual Relationship

```
workflows (1) ─┬─ (N) tasks ─┬─ (N) task_attempts
               │            │
        Every workflow       Every task can
        has many tasks      have many attempts
               │            (if it retries)
         depends_on[...]
               │
        Points back to
        other task IDs
```

### Example: Order Processing

```sql
-- Submit an order workflow
INSERT INTO workflows (id, status) VALUES (workflow-123, 'PENDING');

-- Task 1: Charge the card
INSERT INTO tasks (id, workflow_id, type, payload, depends_on) 
VALUES (task-1, workflow-123, 'charge_card', 
  '{"amount": 100, "cardToken": "tok_visa"}', 
  '{}');  -- No dependencies, runs immediately

-- Task 2: Reserve inventory (depends on charge succeeding)
INSERT INTO tasks (id, workflow_id, type, payload, depends_on) 
VALUES (task-2, workflow-123, 'reserve_inventory', 
  '{"itemIds": [1,2,3]}', 
  ARRAY[task-1]);  -- Waits for task-1

-- Task 3: Send email (depends on inventory reserved)
INSERT INTO tasks (id, workflow_id, type, payload, depends_on) 
VALUES (task-3, workflow-123, 'send_email', 
  '{"to": "customer@example.com"}', 
  ARRAY[task-2]);  -- Waits for task-2

-- Execution happens:
-- 1. Orchestrator sees task-1 has empty depends_on → run it
-- 2. task-1 succeeds, status = SUCCEEDED
-- 3. Orchestrator sees task-2 depends_on=[task-1], task-1 is SUCCEEDED → run it
-- 4. task-2 succeeds
-- 5. Orchestrator sees task-3 depends_on=[task-2], task-2 is SUCCEEDED → run it
-- 6. task-3 succeeds
-- 7. workflow status = COMPLETED
```

### Important Components Highlighted

1. **`tasks.depends_on` (UUID array)**: This is the heart of Relay. It encodes the entire execution graph.

2. **`tasks.status` (enum)**: Tracks where each task is in its lifecycle. The orchestrator only picks tasks with status=PENDING.

3. **`task_attempts` table**: Separates "what task is this" from "how many times did we try it". This is crucial for idempotency (Week 3).

4. **`tasks.payload` (JSONB)**: Flexibility — tasks can have completely different input formats.

5. **Workflow status transitions**: PENDING → RUNNING → (COMPLETED or FAILED). This is the client-facing state.

---

## 4. Why This Design Is Good

✅ **Simple**: Three tables, straightforward relationships  
✅ **Scalable**: Works with Kafka in Week 2; just add a "message published" timestamp  
✅ **Auditable**: Every execution attempt is logged  
✅ **Flexible**: Task types and payloads are plugin-able (add "charge_card", "send_sms", whatever)  
✅ **Reliable**: Postgres ACID guarantees mean your workflow state is never lost or corrupted  

