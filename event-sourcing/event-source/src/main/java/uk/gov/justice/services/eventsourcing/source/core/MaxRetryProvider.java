package uk.gov.justice.services.eventsourcing.source.core;

import uk.gov.justice.services.common.configuration.GlobalValue;

import jakarta.inject.Inject;

public class MaxRetryProvider {

    @Inject
    @GlobalValue(key = "internal.max.retry", defaultValue = "20")
    private long maxRetry;

    public long getMaxRetry() {
        return maxRetry;
    }
}
