package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.time.temporal.ChronoUnit.MILLIS;

import uk.gov.justice.services.event.sourcing.subscription.manager.StreamRetryConfiguration;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

import javax.inject.Inject;

public class RetryTimeCalculator {

    @Inject
    private StreamRetryConfiguration streamRetryConfiguration;

    public ZonedDateTime calculateNextRetryTime(final long retryCount, final ZonedDateTime now) {

        final Long retryDelayMilliseconds = streamRetryConfiguration.getRetryDelayMilliseconds();
        final long retryDelay = new BigDecimal(retryDelayMilliseconds)
                .multiply(new BigDecimal(retryCount))
                .multiply(streamRetryConfiguration.getRetryDelayMultiplier())
                .longValue();

        return now.plus(retryDelay, MILLIS);
    }
}
