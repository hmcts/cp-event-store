package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.util.LazyValue;

import java.math.BigDecimal;

import javax.inject.Inject;

public class StreamRetryConfiguration {

    private final LazyValue maxRetriesLazyValue = new LazyValue();
    private final LazyValue retryDelayLazyValue = new LazyValue();
    private final LazyValue retryDelayMultiplierLazyValue = new LazyValue();

    @Inject
    @Value(key = "event.publishing.stream.processing.max.event.retries", defaultValue = "7")
    private String maxRetries;

    @Inject
    @Value(key = "event.publishing.stream.processing.retry.delay.milliseconds", defaultValue = "1000")
    private String retryDelayMilliseconds;

    @Inject
    @Value(key = "event.publishing.stream.processing.retry.delay.multiplier", defaultValue = "1.0")
    private String retryDelayMultiplier;


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
