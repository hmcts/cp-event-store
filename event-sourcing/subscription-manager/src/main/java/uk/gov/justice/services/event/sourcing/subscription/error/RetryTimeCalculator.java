package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.time.temporal.ChronoUnit.MILLIS;

import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import javax.inject.Inject;

public class RetryTimeCalculator {

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    public ZonedDateTime calculateNextRetryTime(final long retryCount, final ZonedDateTime now) {

        final Long retryDelayMilliseconds = streamProcessingConfig.getRetryDelayMilliseconds();
        final long retryDelay = new BigDecimal(retryDelayMilliseconds)
                .multiply(new BigDecimal(retryCount))
                .multiply(streamProcessingConfig.getRetryDelayMultiplier())
                .longValue();

        return now.plus(retryDelay, MILLIS);
    }
}
