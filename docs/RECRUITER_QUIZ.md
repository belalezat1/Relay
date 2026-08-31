# 5-Question Recruiter Quiz: Relay Project

These are exactly the kinds of questions a recruiter or senior engineer would ask during a portfolio review.

---

## Question 1: The Core Problem
**"Walk me through the problem Relay solves. Why can't you just use a simple message queue like RabbitMQ?"**

### What They're Testing
- Do you understand *why* this problem exists?
- Can you articulate distributed systems challenges?
- Do you understand the trade-offs between simple and sophisticated solutions?

### Good Answer Framework
*"Relay solves the problem of reliably executing multi-step workflows without losing tasks or executing them twice. A naive queue-based system has three issues:*

1. *Worker crashes mid-task — if the worker crashes after executing a payment but before acknowledging the message, the message goes to another worker who might charge the card again (double execution).*

2. *Dependency ordering — imagine an order workflow: charge card → reserve inventory → send email. If inventory fails, a simple queue doesn't know to skip the email. You'd need custom retry logic in each service.*

3. *Failed tasks disappear — if a task fails permanently (bad input, broken downstream API), a simple queue either retries forever (wasting resources) or silently drops it. Either way, you have no visibility.*

*RabbitMQ is great for simple pub/sub, but Relay adds:*
- *Idempotency (using idempotency keys to make retries safe)*
- *Dependency graph resolution (topological sorting)*
- *Dead-letter queues (surface permanent failures for inspection)*

*This makes Relay a proper orchestration engine, not just a broker."*

---

## Question 2: The Trade-Off
**"You're using PostgreSQL for durability and Kafka for distribution. Why not just use Kafka for both? It's a log, so it's durable."**

### What They're Testing
- Do you understand the difference between a **source of truth** and a **distribution mechanism**?
- Can you reason about failure modes?
- Do you know when to trade consistency for scalability?

### Good Answer Framework
*"Kafka is durable, but it's not a system of record for Relay's purposes. Here's why:*

*If a task is only 'recorded as succeeded' by having a message in Kafka, and Kafka loses that message (broker crash, rare), the orchestrator loses visibility. The workflow appears broken without explanation.*

*With PostgreSQL as the source of truth and Kafka as distribution:*
- *Worker picks up task from Kafka*
- *Executes it*
- *Writes result to Postgres (ACID-guaranteed)*
- *If Kafka loses the message, Postgres still has the truth — orchestrator can requeue*

*Postgres is the "system of record"; Kafka is just "how we distribute work". This separation makes the system resilient: Kafka can crash and recover without losing a single workflow's state."*

---

## Question 3: The Dependency Graph
**"How does Relay know which tasks can run in parallel vs. which must run sequentially? Walk me through the algorithm."**

### What They're Testing
- Do you understand the dependency resolution logic?
- Can you explain graph algorithms at a high level?
- Do you think about edge cases (cycles, empty graphs)?

### Good Answer Framework
*"The key is the `depends_on` UUID array in the tasks table. Here's the algorithm:*

1. *Fetch all tasks for a workflow*
2. *Build a directed graph: Task A → Task B means B depends on A*
3. *Detect cycles (DFS with color marking: WHITE/GRAY/BLACK). If we see a GRAY node again, there's a cycle — throw an exception.*
4. *Topologically sort the graph (e.g., Kahn's algorithm). This gives us an execution order.*
5. *Filter for 'ready' tasks: only return tasks where status=PENDING AND all tasks in depends_on have status=SUCCEEDED.*

*Example: A → B → C*
- *Iteration 1: Task A is ready (no deps, status=PENDING) → execute A*
- *A completes, status=SUCCEEDED*
- *Iteration 2: Task B is ready (depends_on=[A], A is SUCCEEDED) → execute B*
- *B completes*
- *Iteration 3: Task C is ready → execute C*
- *Workflow done*

*If we have parallel tasks (A, B both with no deps, then C depends on both):*
- *Iteration 1: Both A and B ready → execute in parallel*
- *Both complete*
- *Iteration 2: C ready (depends_on=[A, B], both SUCCEEDED) → execute C*

*The algorithm naturally handles both linear and parallel DAGs."*

---

## Question 4: The Idempotency Problem
**"You mention 'idempotency keys' in the design doc. What problem do they solve, and how exactly do they prevent double-execution?"**

### What They're Testing
- Do you understand the at-least-once delivery problem?
- Do you know what idempotency means in distributed systems?
- Can you explain the tradeoff between at-least-once and exactly-once?

### Good Answer Framework
*"Idempotency keys solve the double-execution problem in Week 1 and especially in Week 2 when Kafka is involved.*

*The problem:* 
- *Worker receives task from Kafka: 'charge \$100 to card'*
- *Worker executes the charge*
- *Worker crashes before acknowledging the message*
- *Kafka redelivers to another worker*
- *That worker doesn't know the charge already happened — charges again (\$200 instead of \$100)*

*The solution:* 
- *Each task execution gets a unique idempotency key: something like 'task-{task-id}-attempt-{attempt-count}'*
- *Before executing, worker checks: 'Have I already executed this exact key?'*
- *If yes, return the cached result (don't re-execute)*
- *If no, execute and cache the result*

*In the database:*
```sql
task_attempts table:
  idempotency_key (unique)
  result (SUCCESS or FAILURE)
```

*If the message is redelivered with the same key, the worker sees 'this key already has a SUCCESS result' and just acknowledges without re-charging.*

*This turns Kafka's 'at-least-once' delivery (might receive twice) into 'at-most-once side effects' (won't execute twice). That's the magic of idempotency."*

---

## Question 5: Horizontal Scaling
**"You say Relay scales horizontally. Prove it. Describe what happens when you go from 1 worker to 5 workers, and what breaks if you don't design it right."**

### What They're Testing
- Do you think about scalability concretely, not just theoretically?
- Do you understand Kafka consumer groups?
- Do you recognize potential race conditions?
- Do you know about distributed locks?

### Good Answer Framework
*"Horizontal scaling means throughput increases linearly when you add workers (1 worker = 100 tasks/sec, 5 workers = 500 tasks/sec).*

*With Kafka consumer groups, this is mostly automatic:*
- *Worker 1, Worker 2, ..., Worker 5 all join the same Kafka consumer group*
- *Kafka automatically partitions work: if the task topic has 5 partitions, each worker gets 1*
- *Task load balances automatically — no coordination code needed*

*But two things can break if not handled:*

1. *Race condition on task execution:*
   - *Two workers pick up the same task concurrently*
   - *Both start charging the card*
   - *Result: charged twice*
   - *Fix: use Postgres advisory locks (Week 4). Worker acquires lock keyed on task ID before executing. If another worker has it, skip. Advisory locks are built into Postgres, so no extra coordination service needed.*

2. *Idempotency key collision:*
   - *If two workers generate the same idempotency key, they might not detect the duplicate*
   - *Fix: make idempotency keys globally unique and deterministic (e.g., task-{id}-attempt-{attempt-count})*

*So the story is:*
- *Kafka + consumer groups give you load balancing for free (workers auto-discover and partition)*
- *Postgres advisory locks ensure singleton/scheduled tasks run exactly once*
- *Idempotency keys ensure retries are safe*
- *Result: you can scale from 1 worker to 100+ without changing code, and correctness is guaranteed."*

---

## Bonus: What a Good Answer Looks Like

✅ **Shows domain knowledge**: You understand distributed systems (at-least-once, idempotency, consensus)  
✅ **Concrete examples**: You use the order-processing scenario or specific code  
✅ **Acknowledges trade-offs**: "Postgres is not as fast as pure Kafka, but it's more reliable"  
✅ **Thinks about failure modes**: "What if X crashes? How do we recover?"  
✅ **Architecture reasoning**: "We chose this design because..."  

❌ **Avoid**: Vague answers ("it just works"), hand-waving about scale ("Kafka is fast, so it scales"), or treating the design as gospel ("this is the only way")

---

## Follow-Up Questions (If Time)

If they seem interested, they might dig deeper:

- "What's the tradeoff between Week 1 (synchronous) and Week 2 (async)?"
  - *Week 1: simpler to reason about, but blocks the API caller*
  - *Week 2: non-blocking, but adds latency; client can't get result immediately*

- "How would you monitor a production Relay cluster?"
  - *Prometheus metrics: tasks/sec, failure rate, queue depth, idempotency cache hit rate*
  - *Alerting: if dead-letter queue grows, something is permanently broken*

- "What's the hardest part of this project?"
  - *Honest answer: the chaos testing (Week 3). Proving idempotency requires deliberately crashing workers and verifying no double-execution.*

