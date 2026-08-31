# Week 1 Implementation Plan: Relay Core API + Data Model

**Goal**: A single-worker version that can accept and run a workflow end-to-end, with all state persisted to PostgreSQL.

**Deliverable**: A workflow of 3+ dependent tasks submitted via REST API executes correctly, with all statuses and results recorded.

---

## Execution Phases (Sequential Order)

### **Phase 1: Project Setup** (1-2 hours)
**Goal**: Initialize the project structure and local environment.

1. **Create Gradle project structure**
   - Root `build.gradle.kts` with Spring Boot 3.x + version management
   - Submodules: `api` (REST service), `core` (orchestration logic)
   - Dependencies:
     - `spring-boot-starter-web`
     - `spring-boot-starter-data-jpa`
     - `spring-boot-starter-validation`
     - `org.postgresql:postgresql:15+`
     - `org.flywaydb:flyway-core` (for schema migrations)
     - `com.fasterxml.jackson.datatype:jackson-datatype-jsr310` (LocalDateTime support)
     - Test: `junit-jupiter`, `spring-boot-starter-test`

2. **Create `docker-compose.yml` for local development**
   - PostgreSQL 15 service
   - Volume for data persistence
   - Environment: `POSTGRES_DB=relay_dev`, `POSTGRES_USER=relay`, `POSTGRES_PASSWORD=relay_dev`
   - Bound to `localhost:5432`
   - Docs: "Run `docker-compose up -d` before starting the app"

---

### **Phase 2: Database Schema & Migrations** (2-3 hours)
**Goal**: Define and set up the Postgres schema with Flyway.

**File Structure**:
```
src/main/resources/db/migration/
├── V1__Initial_Schema.sql
```

**V1__Initial_Schema.sql** should create:

#### **Enums (Postgres DOMAIN types)**
```sql
CREATE TYPE workflow_status AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED');
CREATE TYPE task_status AS ENUM ('PENDING', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'DEAD_LETTERED');
CREATE TYPE task_result AS ENUM ('SUCCESS', 'FAILURE');
```

#### **Tables**

**workflows**
```sql
CREATE TABLE workflows (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status workflow_status NOT NULL DEFAULT 'PENDING',
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**tasks**
```sql
CREATE TABLE tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  workflow_id UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}',
  status task_status NOT NULL DEFAULT 'PENDING',
  depends_on UUID[] DEFAULT '{}',  -- Array of task IDs this task depends on
  attempt_count INT NOT NULL DEFAULT 0,
  idempotency_key VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_tasks_workflow_id ON tasks(workflow_id);
CREATE INDEX idx_tasks_status ON tasks(status);
```

**task_attempts**
```sql
CREATE TABLE task_attempts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  result task_result,
  error TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_task_attempts_task_id ON task_attempts(task_id);
```

**Indexes & Constraints**:
- Workflow status change triggers `updated_at` update (via trigger or app-side logic)
- Task `depends_on` remains an array; we'll query it in the dependency resolver

---

### **Phase 3: JPA Entity Models** (2-3 hours)
**Goal**: Map the schema to Java entities.

**Package**: `com.relay.core.model`

1. **Workflow.java**
   - `@Entity`, `@Table(name = "workflows")`
   - Fields: `id`, `status`, `createdAt`, `updatedAt`
   - `@Enumerated(EnumType.STRING) WorkflowStatus status`
   - `@OneToMany(mappedBy = "workflow", cascade = CascadeType.ALL, fetch = FetchType.EAGER) List<Task> tasks`
   - Constructors, getters/setters

2. **Task.java**
   - `@Entity`, `@Table(name = "tasks")`
   - Fields: `id`, `workflowId`, `type`, `payload`, `status`, `dependsOn`, `attemptCount`, `idempotencyKey`
   - `@ManyToOne @JoinColumn(name = "workflow_id") Workflow workflow`
   - `payload` → `@Column(columnDefinition = "jsonb") String payload` (we'll use Jackson for serialization)
   - `dependsOn` → `@Column(columnDefinition = "uuid[]") UUID[] dependsOn` or `@ElementCollection` with `@CollectionTable`
   - `@OneToMany(mappedBy = "task", cascade = CascadeType.ALL, fetch = FetchType.EAGER) List<TaskAttempt> attempts`
   - `@Enumerated(EnumType.STRING) TaskStatus status`

3. **TaskAttempt.java**
   - `@Entity`, `@Table(name = "task_attempts")`
   - Fields: `id`, `taskId`, `startedAt`, `finishedAt`, `result`, `error`
   - `@ManyToOne @JoinColumn(name = "task_id") Task task`
   - `@Enumerated(EnumType.STRING) TaskResult result`

4. **Enums**:
   - `WorkflowStatus`: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
   - `TaskStatus`: `PENDING`, `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `DEAD_LETTERED`
   - `TaskResult`: `SUCCESS`, `FAILURE`

---

### **Phase 4: Spring Data Repositories** (1 hour)
**Goal**: Create data access layer.

**Package**: `com.relay.core.repository`

```java
public interface WorkflowRepository extends JpaRepository<Workflow, UUID> {
  // No custom methods needed for MVP
}

public interface TaskRepository extends JpaRepository<Task, UUID> {
  List<Task> findByWorkflowId(UUID workflowId);
  List<Task> findByWorkflowIdAndStatus(UUID workflowId, TaskStatus status);
  // Query: find all tasks in a workflow that are ready to execute
  // (status = PENDING and all dependencies are SUCCEEDED)
}

public interface TaskAttemptRepository extends JpaRepository<TaskAttempt, UUID> {
  TaskAttempt findLatestByTaskId(UUID taskId);
  List<TaskAttempt> findByTaskId(UUID taskId);
}
```

**Note**: We'll implement the "ready to execute" query in the `DependencyGraph` service (Phase 5).

---

### **Phase 5: Core Orchestration Logic** (4-5 hours)
**Goal**: Implement the in-process executor and dependency resolver.

**Package**: `com.relay.core.executor`

#### **5a. Task Executor Interface & Registry**
```java
@FunctionalInterface
public interface TaskExecutor {
  TaskResult execute(Task task) throws Exception;
}

@Component
public class TaskRegistry {
  // Map<String, TaskExecutor>
  // registerExecutor(String type, TaskExecutor executor)
  // getExecutor(String type) -> TaskExecutor or throw UnknownTaskTypeException
}
```

**Sample Test Executors**:
- `EchoTaskExecutor`: logs task input, returns SUCCESS
- `FailingTaskExecutor`: always returns FAILURE (for testing retry logic)
- `SlowTaskExecutor`: sleeps 1s, returns SUCCESS (for testing timing)

#### **5b. Dependency Graph Resolver**
**File**: `DependencyGraphResolver.java`

Responsibilities:
1. Validate the workflow has no cycles (DFS with color marking)
2. Topologically sort tasks
3. Identify which tasks are "ready" (all dependencies succeeded)
4. Return ready tasks in execution order

```java
@Service
public class DependencyGraphResolver {
  public List<Task> getReadyTasks(Workflow workflow) throws CyclicDependencyException {
    // 1. Fetch all tasks for workflow
    // 2. Build adjacency map from depends_on
    // 3. Detect cycles (throw CyclicDependencyException if found)
    // 4. Topologically sort
    // 5. Filter: only tasks with status=PENDING and all dependencies SUCCEEDED
    // 6. Return sorted list
  }
  
  private void detectCycles(Map<UUID, List<UUID>> graph) throws CyclicDependencyException {
    // DFS with WHITE/GRAY/BLACK coloring
  }
  
  private List<UUID> topologicalSort(Map<UUID, List<UUID>> graph) {
    // Kahn's algorithm or DFS-based
  }
}
```

**Unit Tests** (`DependencyGraphResolverTest.java`):
- ✅ Linear chain (A → B → C)
- ✅ Parallel tasks (A, B execute together, then C depends on both)
- ✅ Cycle detection (A → B → A should throw CyclicDependencyException)
- ✅ Ready tasks filtering (only tasks with all deps succeeded)
- ✅ Empty workflow
- ✅ Single task

#### **5c. In-Process Workflow Orchestrator**
**File**: `WorkflowOrchestrator.java`

Responsibilities:
1. Accept a workflow submission (list of task definitions)
2. Persist workflow and tasks to DB
3. Iteratively execute ready tasks until workflow complete or failure
4. Record task results as `TaskAttempt` records
5. Update workflow status

```java
@Service
public class WorkflowOrchestrator {
  // Constructor injection: TaskRepository, WorkflowRepository, TaskAttemptRepository, 
  //                      DependencyGraphResolver, TaskRegistry, TaskExecutor
  
  public Workflow submitAndExecuteWorkflow(WorkflowDefinition definition) 
      throws WorkflowExecutionException {
    // 1. Create Workflow entity (status = PENDING)
    // 2. For each task definition:
    //    a. Create Task entity (status = PENDING, depends_on = [...])
    //    b. Save to DB
    // 3. Update workflow status -> RUNNING
    // 4. Call executeWorkflow(workflow)
    // 5. Return updated workflow
  }
  
  private void executeWorkflow(Workflow workflow) throws WorkflowExecutionException {
    while (workflow.getStatus() != WorkflowStatus.COMPLETED && 
           workflow.getStatus() != WorkflowStatus.FAILED) {
      
      List<Task> readyTasks = dependencyGraphResolver.getReadyTasks(workflow);
      
      if (readyTasks.isEmpty()) {
        // No tasks ready: all tasks either running, succeeded, or failed
        // Check if any failed or all succeeded
        if (allTasksSucceeded(workflow)) {
          workflow.setStatus(WorkflowStatus.COMPLETED);
        } else {
          workflow.setStatus(WorkflowStatus.FAILED);  // at least one task failed
        }
        workflowRepository.save(workflow);
        break;
      }
      
      // Execute each ready task
      for (Task task : readyTasks) {
        executeTask(task);
        task = taskRepository.findById(task.getId()).orElseThrow();
        if (task.getStatus() == TaskStatus.FAILED) {
          // For Week 1: stop workflow on first failure
          workflow.setStatus(WorkflowStatus.FAILED);
          workflowRepository.save(workflow);
          return;
        }
      }
    }
  }
  
  private void executeTask(Task task) {
    TaskAttempt attempt = new TaskAttempt();
    attempt.setTask(task);
    attempt.setStartedAt(LocalDateTime.now());
    
    try {
      TaskExecutor executor = taskRegistry.getExecutor(task.getType());
      TaskResult result = executor.execute(task);
      
      attempt.setResult(TaskResult.SUCCESS);
      attempt.setFinishedAt(LocalDateTime.now());
      task.setStatus(TaskStatus.SUCCEEDED);
      
    } catch (Exception e) {
      attempt.setResult(TaskResult.FAILURE);
      attempt.setError(e.getMessage());
      attempt.setFinishedAt(LocalDateTime.now());
      task.setStatus(TaskStatus.FAILED);
    }
    
    taskAttemptRepository.save(attempt);
    task.setAttemptCount(task.getAttemptCount() + 1);
    taskRepository.save(task);
  }
  
  private boolean allTasksSucceeded(Workflow workflow) {
    return workflow.getTasks().stream()
      .allMatch(t -> t.getStatus() == TaskStatus.SUCCEEDED);
  }
}
```

---

### **Phase 6: REST API Endpoints** (2-3 hours)
**Goal**: Wire up the REST layer.

**Package**: `com.relay.api.controller`

#### **6a. Request/Response DTOs**

**File**: `com.relay.api.dto`

```java
// Request DTOs
@Data
public class TaskDefinition {
  private String type;  // e.g., "charge_card", "reserve_inventory"
  private Map<String, Object> payload;
  private List<String> dependsOn;  // Task names/IDs this depends on
}

@Data
public class WorkflowSubmissionRequest {
  private List<TaskDefinition> tasks;  // Ordered list of task definitions
}

// Response DTOs
@Data
public class TaskResponse {
  private UUID id;
  private String type;
  private TaskStatus status;
  private int attemptCount;
  private List<UUID> dependsOn;
  private List<TaskAttemptResponse> attempts;
}

@Data
public class TaskAttemptResponse {
  private UUID id;
  private LocalDateTime startedAt;
  private LocalDateTime finishedAt;
  private TaskResult result;
  private String error;
}

@Data
public class WorkflowResponse {
  private UUID id;
  private WorkflowStatus status;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
  private List<TaskResponse> tasks;
}
```

#### **6b. REST Controller**

```java
@RestController
@RequestMapping("/workflows")
public class WorkflowController {
  private final WorkflowOrchestrator orchestrator;
  private final WorkflowRepository workflowRepository;
  
  @PostMapping
  public ResponseEntity<WorkflowResponse> submitWorkflow(
      @RequestBody WorkflowSubmissionRequest request) {
    try {
      Workflow workflow = orchestrator.submitAndExecuteWorkflow(
        convertRequestToDefinition(request)
      );
      return ResponseEntity.ok(convertToResponse(workflow));
    } catch (WorkflowExecutionException e) {
      return ResponseEntity.badRequest().build();
    }
  }
  
  @GetMapping("/{id}")
  public ResponseEntity<WorkflowResponse> getWorkflow(@PathVariable UUID id) {
    return workflowRepository.findById(id)
      .map(w -> ResponseEntity.ok(convertToResponse(w)))
      .orElse(ResponseEntity.notFound().build());
  }
  
  // Helper methods to convert entities to DTOs
}
```

**Endpoints**:
1. **POST /workflows**
   - Accept `WorkflowSubmissionRequest`
   - Execute workflow synchronously
   - Return `WorkflowResponse` with final status
   - Status codes: 200 (success), 400 (validation error), 500 (internal error)

2. **GET /workflows/{id}**
   - Return current workflow status + all tasks
   - Status codes: 200 (found), 404 (not found)

---

### **Phase 7: Execution Flow Integration** (1 hour)
**Goal**: Wire POST endpoint to orchestrator.

- **POST /workflows** should call `WorkflowOrchestrator.submitAndExecuteWorkflow()` 
- For Week 1, execution is **synchronous** (blocking) — the API call waits for completion
- Return final `WorkflowResponse` to caller
- Handle exceptions (validation errors, unknown task types, cycles) as 400/500 responses

---

### **Phase 8: Testing** (4-5 hours)
**Goal**: Ensure correctness at all levels.

#### **8a. Unit Tests**

**`DependencyGraphResolverTest.java`** (see Phase 5b):
- Linear chain, parallel tasks, cycle detection, filtering

**`TaskExecutorTest.java`**:
- EchoExecutor returns SUCCESS
- FailingExecutor returns FAILURE
- Executor receives correct task payload

#### **8b. Integration Tests**

**`WorkflowOrchestratorIntegrationTest.java`**:
- Setup: in-memory or `@DataJpaTest` with test Postgres
- Test 1: Submit 3-task linear workflow (A → B → C), verify all execute in order
- Test 2: Parallel tasks (A, B execute together, then C), verify correct ordering
- Test 3: Workflow with failure, verify dependent tasks skipped

**`WorkflowControllerIntegrationTest.java`**:
- Setup: `@SpringBootTest` with test Postgres
- Test 1: POST /workflows with valid payload, verify 200 + response has all tasks
- Test 2: GET /workflows/{id}, verify response matches DB state
- Test 3: POST with invalid workflow (cycle, unknown task type), verify 400
- Test 4: GET non-existent workflow, verify 404

#### **8c. Test Fixtures**

Create utility class `WorkflowTestFixtures.java`:
```java
public class WorkflowTestFixtures {
  public static WorkflowDefinition linearWorkflow() { /* A→B→C */ }
  public static WorkflowDefinition parallelWorkflow() { /* A,B→C */ }
  public static WorkflowDefinition cyclicWorkflow() { /* A→B→A */ }
}
```

---

### **Phase 9: Documentation & Examples** (2 hours)
**Goal**: Make it easy to run and understand.

#### **9a. Example Workflows**

Create `examples/` directory with sample workflow JSON files:

**`order_processing_workflow.json`**:
```json
{
  "tasks": [
    {
      "type": "charge_card",
      "payload": { "amount": 100, "cardToken": "tok_123" },
      "dependsOn": []
    },
    {
      "type": "reserve_inventory",
      "payload": { "itemIds": [1, 2, 3], "quantity": 5 },
      "dependsOn": ["charge_card"]
    },
    {
      "type": "send_email",
      "payload": { "to": "customer@example.com", "template": "order_confirmation" },
      "dependsOn": ["reserve_inventory"]
    }
  ]
}
```

**`parallel_workflow.json`**:
```json
{
  "tasks": [
    {
      "type": "fetch_user_data",
      "payload": { "userId": 42 },
      "dependsOn": []
    },
    {
      "type": "fetch_recommendations",
      "payload": { "userId": 42 },
      "dependsOn": []
    },
    {
      "type": "send_dashboard",
      "payload": { "userId": 42 },
      "dependsOn": ["fetch_user_data", "fetch_recommendations"]
    }
  ]
}
```

#### **9b. README Section: "Getting Started"**

```markdown
### Running Week 1

1. Start PostgreSQL:
   docker-compose up -d

2. Build and run the app:
   ./gradlew build
   ./gradlew bootRun

3. Submit a workflow:
   curl -X POST http://localhost:8080/workflows \
     -H "Content-Type: application/json" \
     -d @examples/order_processing_workflow.json

4. Check status:
   curl http://localhost:8080/workflows/{id}
```

#### **9c. Schema Diagram**

Document the ER diagram in README:
```
Workflows (1) ──── (N) Tasks
                        │
                   depends_on[]
                        │
Tasks (1) ──── (N) TaskAttempts
```

---

### **Phase 10: Validation Checklist** (1-2 hours)
**Goal**: Ensure Week 1 is complete.

- [ ] Gradle builds successfully (`./gradlew build`)
- [ ] All unit tests pass
- [ ] All integration tests pass
- [ ] Docker Compose starts PostgreSQL without errors
- [ ] App starts: `./gradlew bootRun` (server listens on 8080)
- [ ] Example workflow submits via POST /workflows
- [ ] Workflow executes all 3 tasks in correct order
- [ ] GET /workflows/{id} returns final COMPLETED status
- [ ] Parallel workflow test confirms A and B execute concurrently
- [ ] Cycle-detection test throws CyclicDependencyException
- [ ] Failure scenario: one task fails, dependent tasks skipped
- [ ] README documents how to run everything
- [ ] No compilation errors or warnings

---

## Key Design Decisions for Week 1

1. **Synchronous Execution**: Workflows execute inside the POST request. This makes Week 1 simpler; Week 2 splits execution into async via Kafka.

2. **In-Process Executor**: No Kafka yet. Tasks run directly in the orchestrator thread. This validates the core logic before introducing distribution complexity.

3. **Dependency Tracking**: `depends_on` stored as UUID[] in the database and modeled as `@ElementCollection` in JPA. Simple to query and enforce.

4. **Task Payload as JSONB**: Allows flexible, untyped task input. Handled by Spring/Jackson automatically.

5. **Fail-Fast on First Failure**: Week 1 stops the workflow if any task fails. Week 3 adds retries and dead-letter queues.

6. **Enum-based Status**: Postgres DOMAIN types map cleanly to Java enums. Prevents invalid state transitions at the database level.

---

## Estimated Timeline

| Phase | Tasks | Estimated Hours |
|-------|-------|-----------------|
| 1 | Project Setup | 1.5 |
| 2 | Schema & Migrations | 2.5 |
| 3 | JPA Entities | 2.5 |
| 4 | Repositories | 1 |
| 5 | Orchestration Logic | 4.5 |
| 6 | REST Endpoints | 2.5 |
| 7 | Execution Integration | 1 |
| 8 | Testing | 5 |
| 9 | Documentation | 2 |
| 10 | Validation | 1.5 |
| **Total** | | **~24 hours** |

**Parallelizable work**: Phases 1-4 can overlap (project setup while writing entities). Phase 8 (testing) can start as soon as Phase 5 is partially done.

---

## Dependencies & Execution Order

```
setup-gradle
    ↓
setup-docker-env
    ↓
design-schema
    ↓
create-migrations  ←─── entity-workflow ←─── repo-setup ←─── executor-interface ←─── graph-resolver ←─── orchestrator-core
                   ├─── entity-task ←────────────────────────────────────────────────────────────────────────────────┤
                   └─── entity-task-attempt
                        ↓
                   (All parallel after migrations)
                        ↓
endpoint-post-workflow
endpoint-get-workflow
    ↓
execution-trigger
    ↓
unit-test-graph-resolver
unit-test-executor
    ↓
integration-test-workflow
integration-test-api
    ↓
api-schema-doc
example-workflow-test
    ↓
week1-validation
```

---

## Questions for You Before We Start Coding

1. **Gradle or Maven?** (Gradle is more modern; I recommend it)
2. **Spring Boot version?** (3.2.x is current stable)
3. **Any preference on testing framework?** (JUnit 5 + AssertJ recommended)
4. **Database**: Ready to use PostgreSQL, or prefer H2 in-memory for local dev?
5. **Synchronous execution approach**: OK to block the POST request while the workflow runs, or should I add a note about this limitation?

Once you approve this plan, I'll start with Phase 1 (project setup).
