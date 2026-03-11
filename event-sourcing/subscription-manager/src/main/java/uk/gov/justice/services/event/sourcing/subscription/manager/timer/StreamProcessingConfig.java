package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.math.BigDecimal;
import javax.inject.Inject;
import javax.inject.Singleton;
import uk.gov.justice.services.common.configuration.Value;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

@Singleton
public class StreamProcessingConfig {

    @Inject
    @Value(key = "stream.processing.timer.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "stream.processing.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "stream.processing.max.workers", defaultValue = "15")
    private String maxWorkers;

    @Inject
    @Value(key = "stream.processing.idle.threshold.milliseconds", defaultValue = "1000")
    private String idleThresholdMilliseconds;

    @Inject
    @Value(key = "event.publishing.stream.processing.max.event.retries", defaultValue = "7")
    private String maxRetries;

    @Inject
    @Value(key = "event.publishing.stream.processing.retry.delay.milliseconds", defaultValue = "1000")
    private String retryDelayMilliseconds;

    @Inject
    @Value(key = "event.publishing.stream.processing.retry.delay.multiplier", defaultValue = "1.0")
    private String retryDelayMultiplier;

    @Inject
    @Value(key = "pull.mechanism.access.event.store.via.rest", defaultValue = "false")
    private String accessEventStoreViaRest;

    @Inject
    @Value(key = "stream.processing.circuit.breaker.failure.threshold", defaultValue = "50")
    private String circuitBreakerFailureThreshold;

    @Inject
    @Value(key = "stream.processing.circuit.breaker.cooldown.milliseconds", defaultValue = "30000")
    private String circuitBreakerCoolDownMilliseconds;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public int getMaxWorkers() {
        return parseInt(maxWorkers);
    }

    public long getIdleThresholdMilliseconds() {
        return parseLong(idleThresholdMilliseconds);
    }

    public Integer getMaxRetries() {
        return parseInt(maxRetries);
    }

    public Long getRetryDelayMilliseconds() {
        return parseLong(retryDelayMilliseconds);
    }

    public BigDecimal getRetryDelayMultiplier() {
        return new BigDecimal(retryDelayMultiplier);
    }

    public boolean accessEventStoreViaRest() {
        return parseBoolean(accessEventStoreViaRest);
    }

    public int getCircuitBreakerFailureThreshold() {
        return parseInt(circuitBreakerFailureThreshold);
    }

    public long getCircuitBreakerCoolDownMilliseconds() {
        return parseLong(circuitBreakerCoolDownMilliseconds);
    }
}
