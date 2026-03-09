package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class NextEventSelector {

    @Inject
    private NextEventReader nextEventReader;

    public Optional<PulledEvent> selectNextEvent(
            final String source,
            final Optional<LockedStreamStatus> lockedStreamStatus) {

        if (lockedStreamStatus.isPresent()) {
            final JsonEnvelope eventJsonEnvelope = findNextEventInTheStreamAfterPosition(source, lockedStreamStatus.get());
            return Optional.of(new PulledEvent(eventJsonEnvelope, lockedStreamStatus.get()));
        }

        return Optional.empty();
    }

    private JsonEnvelope findNextEventInTheStreamAfterPosition(
            final String source,
            final LockedStreamStatus lockedStreamStatus) {

        final UUID streamId = lockedStreamStatus.streamId();
        final Long position = lockedStreamStatus.position();
        final Long latestKnownPosition = lockedStreamStatus.latestKnownPosition();

        final Optional<JsonEnvelope> eventJsonEnvelope;

        try {
            eventJsonEnvelope = nextEventReader.read(streamId, position, source);
        } catch (final Exception e) {
            throw new StreamProcessingException(
                    format("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition), e);
        }

        return eventJsonEnvelope.orElseThrow(() ->
                new StreamProcessingException(
                        format("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d", streamId, position, latestKnownPosition)));
    }

    public record PulledEvent(JsonEnvelope jsonEnvelope, LockedStreamStatus lockedStreamStatus) {
    }
}