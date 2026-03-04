package uk.gov.justice.services.event.sourcing.subscription.error;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetry;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorRetryRepository;

import java.sql.Connection;
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
        final StreamErrorRetry streamErrorRetry = buildIncrementedRetry(streamId, source, component, retryCount);
        streamErrorRetryRepository.upsert(streamErrorRetry);
    }

    public void updateStreamRetryCountAndNextRetryTime(
            final UUID streamId,
            final String source,
            final String component,
            final Connection connection) {

        final Long retryCount = streamErrorRetryRepository.getRetryCount(streamId, source, component, connection);
        final StreamErrorRetry streamErrorRetry = buildIncrementedRetry(streamId, source, component, retryCount);
        streamErrorRetryRepository.upsert(streamErrorRetry, connection);
    }

    private StreamErrorRetry buildIncrementedRetry(
            final UUID streamId,
            final String source,
            final String component,
            final long currentRetryCount) {

        final long incrementedRetryCount = currentRetryCount + 1;
        final ZonedDateTime now = clock.now();
        final ZonedDateTime nextRetryTime = retryTimeCalculator.calculateNextRetryTime(incrementedRetryCount, now);

        return new StreamErrorRetry(
                streamId,
                source,
                component,
                incrementedRetryCount,
                nextRetryTime
        );
    }

    public void removeStreamRetryStatus(final UUID streamId, final String source, final String component) {

        streamErrorRetryRepository.remove(streamId, source, component);
    }
}
