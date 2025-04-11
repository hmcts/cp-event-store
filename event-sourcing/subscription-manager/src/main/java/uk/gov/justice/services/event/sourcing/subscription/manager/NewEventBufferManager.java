package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static java.util.Optional.of;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class NewEventBufferManager {

    @Inject
    private NewEventBufferRepository newEventBufferRepository;

    @Inject
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    private BufferedEventFactory bufferedEventFactory;

    @Inject
    private UtcClock clock;

    public Optional<JsonEnvelope> getNextFromEventBuffer(
            final JsonEnvelope incomingJsonEnvelope,
            final String componentName) {

        final Metadata metadata = incomingJsonEnvelope.metadata();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(
                format("No streamId found in event. name '%s', eventId '%s'",
                        metadata.name(),
                        metadata.id())));
        final String source = metadata.source().orElseThrow(() -> new MissingSourceException(
                format("No source found in event. name '%s', eventId '%s'",
                        metadata.name(),
                        metadata.id())));

        final Optional<EventBufferEvent> nextEvent = newEventBufferRepository.findNextForStream(
                streamId,
                source,
                componentName);

        if (nextEvent.isEmpty()) {
            return of(incomingJsonEnvelope);
        }

        return nextEvent.map(streamBufferEvent -> jsonObjectEnvelopeConverter.asEnvelope(streamBufferEvent.getEvent()));
    }

    public void addToBuffer(final JsonEnvelope incomingJsonEnvelope, final String componentName) {

        final EventBufferEvent eventBufferEvent = bufferedEventFactory.createFrom(
                incomingJsonEnvelope,
                componentName,
                clock.now());
        
        newEventBufferRepository.buffer(eventBufferEvent);
    }
}
