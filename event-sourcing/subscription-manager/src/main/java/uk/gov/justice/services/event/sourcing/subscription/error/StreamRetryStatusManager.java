package uk.gov.justice.services.event.sourcing.subscription.error;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.inject.Inject;

public class StreamRetryStatusManager {

    @Inject
    private StreamErrorRetryRepository streamErrorRetryRepository;

    @Inject
    private RetryTimeCalculator retryTimeCalculator;

    @Inject
    private UtcClock clock;

    public void updateStreamRetryCountAndNextRetryTime(
            final UUID streamId,
            final String source,
            final String component) {

        final Long retryCount = streamErrorRetryRepository.getRetryCount(streamId, source, component);
        final long incrementedRetryCount = retryCount + 1;

        final ZonedDateTime now = clock.now();
        final ZonedDateTime nextRetryTime = retryTimeCalculator.calculateNextRetryTime(incrementedRetryCount, now);

        final StreamErrorRetry streamErrorRetry = new StreamErrorRetry(
                streamId,
                source,
                component,
                incrementedRetryCount,
                nextRetryTime
        );

        streamErrorRetryRepository.upsert(streamErrorRetry);
    }



    public void removeStreamRetryStatus(final UUID streamId, final String source, final String component) {

        streamErrorRetryRepository.remove(streamId, source, component);
    }
}
