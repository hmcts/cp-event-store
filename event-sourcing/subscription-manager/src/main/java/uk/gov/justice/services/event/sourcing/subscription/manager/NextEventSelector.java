package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.UserTransaction;

public class NextEventSelector {

    @Inject
    private NextEventReader nextEventReader;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private UserTransaction userTransaction;

    public Optional<PulledEvent> selectNextEvent(
            final String source,
            final String component,
            final Optional<LockedStreamStatus> lockedStreamStatus) {

        if (lockedStreamStatus.isPresent()) {
            final JsonEnvelope eventJsonEnvelope = findNextEventInTheStreamAfterPosition(source, component, lockedStreamStatus.get());
            return Optional.of(new PulledEvent(eventJsonEnvelope, lockedStreamStatus.get()));
        }

        return Optional.empty();
    }

    private JsonEnvelope findNextEventInTheStreamAfterPosition(
            final String source,
            final String component,
            final LockedStreamStatus lockedStreamStatus) {

        final Optional<JsonEnvelope> eventJsonEnvelope;
        final UUID streamId = lockedStreamStatus.streamId();
        final Long position = lockedStreamStatus.position();
        final Long latestKnownPosition = lockedStreamStatus.latestKnownPosition();

        try {
            eventJsonEnvelope = nextEventReader.read(streamId, position);
        } catch (Exception e) {
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(
                    format("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition));
        }

        //TODO revisit this later to understand the requirement on whether to mark the stream as failed if this ever happens, but with current db schema without an event stream can not be marked as error
        return eventJsonEnvelope.orElseThrow(() -> {
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            transactionHandler.rollback(userTransaction);
            throw new StreamProcessingException(
                    format("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition));
        });
    }

    public record PulledEvent(JsonEnvelope jsonEnvelope, LockedStreamStatus lockedStreamStatus) {
    }
}
