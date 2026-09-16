package com.relay.api;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Optional Postgres IT via Testcontainers.
 * <p>
 * Disabled for v1 close-out: Phase IX evidence is {@code mvn test} (H2) plus
 * GitHub Actions Compose smoke ({@code scripts/kafka-smoke.sh}). Re-enable when
 * a Docker-backed IT job is intentionally added.
 */
@Disabled("v1 close-out uses GHA Compose smoke instead of Testcontainers")
class PhaseIXIntegrationTest {

    @Test
    void placeholder() {
        // Intentionally empty — class retained so historical references still resolve.
    }
}
