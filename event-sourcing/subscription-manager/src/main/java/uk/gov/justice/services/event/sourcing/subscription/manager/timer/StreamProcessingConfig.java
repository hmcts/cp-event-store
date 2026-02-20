package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.util.LazyValue;

import java.math.BigDecimal;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StreamProcessingConfig {

    private final LazyValue maxRetriesLazyValue = new LazyValue();
    private final LazyValue retryDelayLazyValue = new LazyValue();
    private final LazyValue retryDelayMultiplierLazyValue = new LazyValue();

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
        return maxRetriesLazyValue.createIfAbsent(() -> parseInt(maxRetries));
    }

    public Long getRetryDelayMilliseconds() {
        return retryDelayLazyValue.createIfAbsent(() -> parseLong(retryDelayMilliseconds));
    }

    public BigDecimal getRetryDelayMultiplier() {
        return retryDelayMultiplierLazyValue.createIfAbsent(() -> new BigDecimal(retryDelayMultiplier));
    }
}
