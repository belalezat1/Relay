package com.relay.api.bench;

import com.relay.api.RelayApplication;
import com.relay.core.model.TaskDefinition;
import com.relay.core.model.TaskStatus;
import com.relay.core.repository.OutboxEventRepository;
import com.relay.core.repository.TaskRepository;
import com.relay.core.service.WorkflowOrchestrator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Resume-defensible throughput bench: Kafka consumer-group workers on a fixed 2-node DAG.
 * Invoked by {@code scripts/benchmark.sh}; excluded from default {@code mvn test}.
 */
@Tag("benchmark")
@EnabledIfEnvironmentVariable(named = "RELAY_BENCH", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResumeThroughputBenchmarkIT {

    private static final int WARMUP_DAGS = intSetting("relay.bench.warmup-dags", "WARMUP_DAGS", 40);
    private static final int MEASURED_DAGS = intSetting("relay.bench.measured-dags", "MEASURED_DAGS", 600);
    private static final int TASKS_PER_DAG = 2;
    private static final int SUBMIT_THREADS = intSetting("relay.bench.submit-threads", "SUBMIT_THREADS", 24);

    private static int intSetting(String property, String env, int defaultValue) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank()) {
            value = System.getenv(env);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    @Test
    void measureOneAndThreeKafkaWorkersAndWriteDocs() throws Exception {
        if (dockerAvailable()) {
            measureWithTestcontainers();
            return;
        }
        measureWithEmbeddedKafka();
    }

    private void measureWithTestcontainers() throws Exception {
        PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("relay_bench")
            .withUsername("relay")
            .withPassword("relay");
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));
        postgres.start();
        kafka.start();
        try {
            Cluster cluster = new Cluster(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword(),
                "org.postgresql.Driver",
                kafka.getBootstrapServers(),
                true,
                "org.hibernate.dialect.PostgreSQLDialect",
                true,
                "Testcontainers Postgres 15 + Kafka 3.8.1 (Docker)"
            );
            writeResults(runBothWorkerModes(cluster), cluster.runtimeNote());
        } finally {
            kafka.stop();
            postgres.stop();
        }
    }

    private void measureWithEmbeddedKafka() throws Exception {
        EmbeddedKafkaBroker kafka = new EmbeddedKafkaKraftBroker(1, 12,
            "relay.workflow.tasks",
            "relay.workflow.tasks.retry",
            "relay.workflow.events"
        );
        kafka.afterPropertiesSet();
        try {
            Cluster cluster = new Cluster(
                "jdbc:h2:mem:relay_bench;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH",
                "sa",
                "",
                "org.h2.Driver",
                kafka.getBrokersAsString(),
                false,
                "org.hibernate.dialect.H2Dialect",
                false,
                "Embedded Kafka (12 partitions) + in-process H2 (Docker not running)"
            );
            writeResults(runBothWorkerModes(cluster), cluster.runtimeNote());
        } finally {
            kafka.destroy();
        }
    }

    private List<RunResult> runBothWorkerModes(Cluster cluster) throws Exception {
        List<RunResult> results = new ArrayList<>();
        results.add(runWorkerMode(cluster, 1));
        results.add(runWorkerMode(cluster, 3));
        return results;
    }

    private RunResult runWorkerMode(Cluster cluster, int workers) throws Exception {
        String groupId = "bench-task-w" + workers + "-" + UUID.randomUUID();
        ConfigurableApplicationContext producer = startApp(cluster, 0, false, groupId, true);
        ConfigurableApplicationContext workersCtx = startApp(cluster, workers, true, groupId, false);
        try {
            WorkflowOrchestrator orchestrator = producer.getBean(WorkflowOrchestrator.class);
            TaskRepository tasks = producer.getBean(TaskRepository.class);
            OutboxEventRepository outbox = producer.getBean(OutboxEventRepository.class);
            org.springframework.kafka.config.KafkaListenerEndpointRegistry registry =
                workersCtx.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class);

            long succeededBeforeWarmup = tasks.countByStatus(TaskStatus.SUCCEEDED);
            submitDags(orchestrator, WARMUP_DAGS);
            waitForOutboxDrain(outbox, 60_000);
            startListeners(registry);
            waitForSucceeded(tasks, succeededBeforeWarmup + (long) WARMUP_DAGS * TASKS_PER_DAG, 120_000);
            stopListeners(registry);

            long succeededBefore = tasks.countByStatus(TaskStatus.SUCCEEDED);
            submitDags(orchestrator, MEASURED_DAGS);
            waitForOutboxDrain(outbox, 60_000);

            long expected = succeededBefore + (long) MEASURED_DAGS * TASKS_PER_DAG;
            List<Long> completionLatenciesMs = new ArrayList<>();
            long startedAt = System.nanoTime();
            long windowStartMs = System.currentTimeMillis();
            startListeners(registry);
            waitForSucceededWithLatencies(tasks, expected, 180_000, windowStartMs, completionLatenciesMs, succeededBefore);
            double elapsedSec = (System.nanoTime() - startedAt) / 1_000_000_000.0;
            if (elapsedSec <= 0) {
                elapsedSec = 0.001;
            }
            int measuredTasks = MEASURED_DAGS * TASKS_PER_DAG;
            double rate = measuredTasks / elapsedSec;
            double p50 = percentile(completionLatenciesMs, 0.50);
            double p95 = percentile(completionLatenciesMs, 0.95);
            System.out.printf(Locale.US, "workers=%d tasks=%d elapsed=%.3fs rate=%.2f tasks/sec p50=%.0fms p95=%.0fms%n",
                workers, measuredTasks, elapsedSec, rate, p50, p95);
            return new RunResult(workers, measuredTasks, elapsedSec, rate, p50, p95);
        } finally {
            workersCtx.close();
            producer.close();
        }
    }

    private ConfigurableApplicationContext startApp(
        Cluster cluster,
        int workers,
        boolean consumeTasks,
        String groupId,
        boolean listenerAutoStartup
    ) {
        SpringApplication application = new SpringApplication(RelayApplication.class);
        application.setLogStartupInfo(false);
        return application.run(
            "--spring.main.banner-mode=off",
            "--server.port=0",
            "--APP_ENV=test",
            "--spring.profiles.active=test",
            "--spring.datasource.url=" + cluster.jdbcUrl(),
            "--spring.datasource.username=" + cluster.username(),
            "--spring.datasource.password=" + cluster.password(),
            "--spring.datasource.driver-class-name=" + cluster.driver(),
            "--spring.jpa.hibernate.ddl-auto=" + (cluster.flyway() ? "validate" : "update"),
            "--spring.jpa.show-sql=false",
            "--spring.jpa.properties.hibernate.format_sql=false",
            "--spring.jpa.properties.hibernate.dialect=" + cluster.dialect(),
            "--spring.flyway.enabled=" + cluster.flyway(),
            "--spring.datasource.hikari.maximum-pool-size=32",
            "--relay.kafka.enabled=true",
            "--relay.kafka.bootstrap-servers=" + cluster.brokers(),
            "--spring.kafka.bootstrap-servers=" + cluster.brokers(),
            "--relay.kafka.task-consumer.enabled=" + consumeTasks,
            "--relay.kafka.task-consumer.concurrency=" + Math.max(1, workers),
            "--relay.kafka.task-consumer.group-id=" + groupId,
            "--relay.kafka.topic-partitions=12",
            "--relay.worker.orchestration-enabled=true",
            "--relay.worker.poll-delay=50",
            "--relay.worker.max-concurrency=16",
            "--relay.worker.batch-size=200",
            "--relay.outbox.poll-delay=20",
            "--relay.outbox.batch-size=200",
            "--relay.task.claim.skip-locked=" + cluster.skipLocked(),
            "--relay.task.lease-recovery-delay=3600000",
            "--relay.kafka.lag-poll-delay=3600000",
            "--relay.retry.backoff-enabled=false",
            "--relay.kafka.lifecycle-events.enabled=false",
            "--spring.kafka.listener.auto-startup=" + listenerAutoStartup,
            "--spring.kafka.consumer.auto-offset-reset=earliest",
            "--spring.kafka.consumer.max-poll-records=32",
            "--logging.level.root=WARN",
            "--logging.level.com.relay=WARN",
            "--logging.level.org.hibernate.SQL=WARN",
            "--management.health.kafka.enabled=false"
        );
    }

    private void startListeners(org.springframework.kafka.config.KafkaListenerEndpointRegistry registry) {
        registry.getListenerContainers().forEach(container -> {
            if (!container.isRunning()) {
                container.start();
            }
        });
    }

    private void stopListeners(org.springframework.kafka.config.KafkaListenerEndpointRegistry registry) {
        registry.getListenerContainers().forEach(container -> {
            if (container.isRunning()) {
                container.stop();
            }
        });
    }

    private void submitDags(WorkflowOrchestrator orchestrator, int count) {
        ExecutorService pool = Executors.newFixedThreadPool(SUBMIT_THREADS);
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    TaskDefinition first = new TaskDefinition();
                    first.setId("task-a");
                    first.setType("success");
                    first.setPayload(Map.of());
                    TaskDefinition second = new TaskDefinition();
                    second.setId("task-b");
                    second.setType("success");
                    second.setDependsOn(List.of("task-a"));
                    second.setPayload(Map.of());
                    orchestrator.createAndExecuteWorkflow(List.of(first, second));
                }, pool));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        } finally {
            pool.shutdownNow();
        }
    }

    private void waitForOutboxDrain(OutboxEventRepository outbox, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (outbox.countByStatus("PENDING") == 0) {
                return;
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("Outbox did not drain in time; pending=" + outbox.countByStatus("PENDING"));
    }

    private void waitForSucceeded(TaskRepository tasks, long expected, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (tasks.countByStatus(TaskStatus.SUCCEEDED) >= expected) {
                return;
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException(
            "Timed out waiting for " + expected + " SUCCEEDED tasks; saw " + tasks.countByStatus(TaskStatus.SUCCEEDED)
        );
    }

    private void waitForSucceededWithLatencies(
        TaskRepository tasks,
        long expected,
        long timeoutMs,
        long windowStartMs,
        List<Long> latenciesMs,
        long succeededBefore
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long last = succeededBefore;
        while (System.currentTimeMillis() < deadline) {
            long succeeded = tasks.countByStatus(TaskStatus.SUCCEEDED);
            if (succeeded > last) {
                long latency = Math.max(0L, System.currentTimeMillis() - windowStartMs);
                for (long i = last; i < succeeded; i++) {
                    latenciesMs.add(latency);
                }
                last = succeeded;
            }
            if (succeeded >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException(
            "Timed out waiting for " + expected + " SUCCEEDED tasks; saw " + tasks.countByStatus(TaskStatus.SUCCEEDED)
        );
    }

    private static double percentile(List<Long> values, double p) {
        if (values == null || values.isEmpty()) {
            return 0;
        }
        List<Long> sorted = values.stream().sorted().toList();
        int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(p * sorted.size()) - 1));
        return sorted.get(index);
    }

    private void writeResults(List<RunResult> results, String runtimeNote) throws Exception {
        writeMarkdown(results, runtimeNote, null);
    }

    private void writeMarkdown(List<RunResult> results, String runtimeNote, String blocker) throws Exception {
        Path output = resolveOutput();
        Files.createDirectories(output.getParent());
        String generatedAt = DateTimeFormatter.ISO_INSTANT.format(Instant.now().atOffset(ZoneOffset.UTC));
        String hardware = hardwareNote();
        StringBuilder md = new StringBuilder();
        md.append("# Relay benchmarks\n\n");
        md.append("Generated by `scripts/benchmark.sh` on ").append(generatedAt).append(".\n\n");
        md.append("## Workload\n\n");
        md.append("- Fixed DAG: `task-a (success) -> task-b (success)` (2 tasks / workflow)\n");
        md.append("- Kafka enabled; Postgres remains the production source of truth; dispatch via transactional outbox\n");
        md.append("- Workers = competing Kafka consumers (`relay.kafka.task-consumer.concurrency`), ");
        md.append("equivalent to `docker compose --profile distributed --scale relay-worker=N`\n");
        md.append("- Warm-up: ").append(WARMUP_DAGS).append(" DAGs; measured: ").append(MEASURED_DAGS);
        md.append(" DAGs (").append(MEASURED_DAGS * TASKS_PER_DAG).append(" tasks)\n");
        md.append("- Measured window: Kafka listeners start (already-produced backlog) → all measured tasks `SUCCEEDED`\n");
        md.append("- Kafka lifecycle events off for this isolation run (Postgres audit rows still written); task dispatch still uses the outbox\n");
        if (runtimeNote != null) {
            md.append("- Runtime: ").append(runtimeNote).append("\n");
        }
        md.append("- Commands:\n\n");
        md.append("```bash\n");
        md.append("export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home\n");
        md.append("./scripts/benchmark.sh\n");
        md.append("```\n\n");
        md.append("Hardware: ").append(hardware).append("\n\n");
        md.append("## Results\n\n");
        md.append("| Scenario | Workers | Tasks | Duration | Throughput | p50 complete | p95 complete | Source |\n");
        md.append("| --- | ---: | ---: | --- | --- | --- | --- | --- |\n");
        if (results == null || results.isEmpty()) {
            md.append("| kafka 1-worker | 1 | ").append(MEASURED_DAGS * TASKS_PER_DAG).append(" | — | — | — | — | run locally |\n");
            md.append("| kafka 3-worker | 3 | ").append(MEASURED_DAGS * TASKS_PER_DAG).append(" | — | — | — | — | run locally |\n");
            if (blocker != null) {
                md.append("\n").append(blocker).append("\n");
            }
        } else {
            for (RunResult result : results) {
                md.append(String.format(
                    Locale.US,
                    "| kafka %d-worker | %d | %d | %.3fs | **%.2f tasks/sec** | %.0fms | %.0fms | measured |%n",
                    result.workers,
                    result.workers,
                    result.tasks,
                    result.elapsedSec,
                    result.rate,
                    result.p50Ms,
                    result.p95Ms
                ));
            }
            RunResult one = results.stream().filter(r -> r.workers == 1).findFirst().orElse(null);
            RunResult three = results.stream().filter(r -> r.workers == 3).findFirst().orElse(null);
            md.append("\n## Resume claims\n\n");
            if (one != null && three != null) {
                double scale = one.rate <= 0 ? 0 : three.rate / one.rate;
                md.append(String.format(Locale.US, "- 1→3 worker scale factor: **%.2f×** (target ~3×)%n", scale));
                md.append(String.format(Locale.US, "- Peak measured throughput: **%.2f tasks/sec** (target 500+/s)%n",
                    Math.max(one.rate, three.rate)));
                if (Math.max(one.rate, three.rate) < 500) {
                    md.append("- 500+/s **not hit on this hardware** with the documented DAG; ");
                    md.append("reproduce with the command above. Do not invent a higher number.\n");
                } else {
                    md.append("- 500+/s **hit** on the Kafka consumer-group path for this run.\n");
                }
                if (scale < 2.4) {
                    md.append(String.format(
                        Locale.US,
                        "- ~3× scale-up **not hit** (measured %.2f×). Bottleneck is likely shared DB/outbox rather than partition assignment; numbers above are the source of truth.%n",
                        scale
                    ));
                } else {
                    md.append("- ~3× scale-up **observed** (within a realistic competing-consumer band).\n");
                }
            }
        }
        md.append("\n## Related automated evidence (`mvn test`)\n\n");
        md.append("- `ResumeReliabilityClaimsTest.zeroDuplicateSideEffectsAcrossOneThousandCrashAndRedeliveryInjections`\n");
        md.append("- `ResumeReliabilityClaimsTest.oneHundredPercentOfExhaustedRetriesLandInInspectableDlqWithReplay`\n");
        md.append("- `KafkaReliabilityTest.concurrentConsumersOnlyExecuteOnce` / `duplicateDeliveryOfSucceededTaskIsANoOp`\n");
        md.append("- `ApiControllerTest.listDeadLettersReplayAndDispatchFailures`\n");
        Files.writeString(output, md.toString());
        System.out.println("Wrote " + output.toAbsolutePath());
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException ex) {
            return false;
        }
    }

    private static Path resolveOutput() {
        String override = System.getProperty("relay.bench.output");
        if (override == null || override.isBlank()) {
            override = System.getenv("RELAY_BENCH_OUTPUT");
        }
        if (override != null && !override.isBlank()) {
            return Path.of(override);
        }
        Path cwd = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        Path direct = cwd.resolve("docs/benchmarks.md");
        if (Files.isDirectory(cwd.resolve("docs"))) {
            return direct;
        }
        return cwd.getParent() == null ? direct : cwd.getParent().resolve("docs/benchmarks.md");
    }

    private static String hardwareNote() {
        return System.getProperty("os.name")
            + " "
            + System.getProperty("os.arch")
            + ", "
            + Runtime.getRuntime().availableProcessors()
            + " CPUs, Java "
            + System.getProperty("java.version");
    }

    private record Cluster(
        String jdbcUrl,
        String username,
        String password,
        String driver,
        String brokers,
        boolean flyway,
        String dialect,
        boolean skipLocked,
        String runtimeNote
    ) {
    }

    private record RunResult(int workers, int tasks, double elapsedSec, double rate, double p50Ms, double p95Ms) {
    }
}
