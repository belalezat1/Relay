package com.relay.core.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {

    @Test
    void defaultsEnableBackoffWithJitterAndConfigurableMaxAttempts() {
        RetryPolicy policy = new RetryPolicy(true, 5, 5L, 2L, 0.2d);
        assertThat(policy.isBackoffEnabled()).isTrue();
        assertThat(policy.getConfiguredMaxAttempts()).isEqualTo(5);
        assertThat(policy.getJitterFraction()).isEqualTo(0.2d);

        long delay = policy.getRetryDelaySeconds(new com.relay.core.model.Task(), 2);
        assertThat(delay).isBetween(4L, 6L);
    }

    @Test
    void disabledBackoffReturnsZeroDelay() {
        RetryPolicy policy = new RetryPolicy(false, 3, 5L, 2L, 0.2d);
        assertThat(policy.getRetryDelaySeconds(new com.relay.core.model.Task(), 3)).isZero();
    }

    @Test
    void jitterKeepsDelayPositive() {
        RetryPolicy policy = new RetryPolicy();
        for (int i = 0; i < 50; i++) {
            assertThat(policy.applyJitter(5L)).isPositive();
        }
    }
}
