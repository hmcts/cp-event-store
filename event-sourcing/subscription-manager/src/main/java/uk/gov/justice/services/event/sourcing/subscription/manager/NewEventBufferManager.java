package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static java.util.Optional.empty;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
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

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    public Optional<JsonEnvelope> getNextFromEventBuffer(
            final JsonEnvelope incomingJsonEnvelope,
            final String componentName) {

        final Metadata metadata = incomingJsonEnvelope.metadata();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(
                format("No streamId found in event. name '%s', eventId '%s'",
                        metadata.name(),
                        metadata.id())));
        final String source = eventSourceNameCalculator.getSource(incomingJsonEnvelope);
        final long currentPosition = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(
                format("No position found in event. name '%s', eventId '%s'",
                        metadata.name(),
                        metadata.id())));

        final long nextPosition = currentPosition + 1;

        final Optional<EventBufferEvent> nextEvent = newEventBufferRepository.findByPositionAndStream(
                streamId,
                nextPosition,
                source,
                componentName);

        if (nextEvent.isPresent()) {
            return nextEvent.map(streamBufferEvent -> jsonObjectEnvelopeConverter.asEnvelope(streamBufferEvent.getEvent()));
        }

        return empty();

    }

    public void addToBuffer(final JsonEnvelope incomingJsonEnvelope, final String componentName) {

        final EventBufferEvent eventBufferEvent = bufferedEventFactory.createFrom(
                incomingJsonEnvelope,
                componentName,
                clock.now());

        newEventBufferRepository.insert(eventBufferEvent);
    }
}
