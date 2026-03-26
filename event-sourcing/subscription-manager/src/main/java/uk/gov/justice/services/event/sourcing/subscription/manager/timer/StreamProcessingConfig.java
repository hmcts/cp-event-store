package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.math.BigDecimal;
import javax.inject.Inject;
import javax.inject.Singleton;
import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.common.util.LazyValue;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

@Singleton
public class StreamProcessingConfig {

    private final LazyValue timerStartWaitLazyValue = new LazyValue();
    private final LazyValue timerIntervalLazyValue = new LazyValue();
    private final LazyValue maxWorkersLazyValue = new LazyValue();
    private final LazyValue idleThresholdLazyValue = new LazyValue();
    private final LazyValue maxRetriesLazyValue = new LazyValue();
    private final LazyValue retryDelayLazyValue = new LazyValue();
    private final LazyValue retryDelayMultiplierLazyValue = new LazyValue();
    private final LazyValue accessEventStoreViaRestLazyValue = new LazyValue();
    private final LazyValue circuitBreakerFailureThresholdLazyValue = new LazyValue();
    private final LazyValue circuitBreakerCoolDownLazyValue = new LazyValue();
    private final LazyValue discoveryNotifiedLazyValue = new LazyValue();

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Inject
    @Value(key = "stream.processing.discovery.notified", defaultValue = "false")
    private String discoveryNotified;

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
    @Value(key = "stream.processing.max.event.retries", defaultValue = "7")
    private String maxRetries;

    @Inject
    @Value(key = "stream.processing.retry.delay.milliseconds", defaultValue = "1000")
    private String retryDelayMilliseconds;

    @Inject
    @Value(key = "stream.processing.retry.delay.multiplier", defaultValue = "1.0")
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
        return timerStartWaitLazyValue.createIfAbsent(() -> parseLong(timerStartWaitMilliseconds));
    }

    public long getTimerIntervalMilliseconds() {
        return timerIntervalLazyValue.createIfAbsent(() -> parseLong(timerIntervalMilliseconds));
    }

    public int getMaxWorkers() {
        return maxWorkersLazyValue.createIfAbsent(() -> parseInt(maxWorkers));
    }

    public long getIdleThresholdMilliseconds() {
        return idleThresholdLazyValue.createIfAbsent(() -> parseLong(idleThresholdMilliseconds));
    }

    public Integer getMaxRetries() {
        return maxRetriesLazyValue.createIfAbsent(() -> parseInt(maxRetries));
    }

    public Long getRetryDelayMilliseconds() {
        return retryDelayLazyValue.createIfAbsent(() -> parseLong(retryDelayMilliseconds));
    }

    public BigDecimal getRetryDelayMultiplier() {
        return retryDelayMultiplierLazyValue.createIfAbsent(() -> new BigDecimal(retryDelayMultiplier));
    }

    public boolean accessEventStoreViaRest() {
        return accessEventStoreViaRestLazyValue.createIfAbsent(() -> parseBoolean(accessEventStoreViaRest));
    }

    public int getCircuitBreakerFailureThreshold() {
        return circuitBreakerFailureThresholdLazyValue.createIfAbsent(() -> parseInt(circuitBreakerFailureThreshold));
    }

    public long getCircuitBreakerCoolDownMilliseconds() {
        return circuitBreakerCoolDownLazyValue.createIfAbsent(() -> parseLong(circuitBreakerCoolDownMilliseconds));
    }

    public boolean shouldDiscoveryNotified() {
        return eventPullConfiguration.shouldProcessEventsByPullMechanism()
                && discoveryNotifiedLazyValue.createIfAbsent(() -> parseBoolean(discoveryNotified));
    }
}
