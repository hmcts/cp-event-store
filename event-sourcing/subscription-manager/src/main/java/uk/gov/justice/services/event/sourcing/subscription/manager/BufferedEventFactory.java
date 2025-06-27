package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.inject.Inject;

public class BufferedEventFactory {

    @Inject
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    public EventBufferEvent createFrom(final JsonEnvelope jsonEnvelope, final String componentName, final ZonedDateTime bufferedAt) {

        final Metadata metadata = jsonEnvelope.metadata();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(format("No streamId found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));
        final String source = eventSourceNameCalculator.getSource(jsonEnvelope);
        final Long positionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No positionInStream found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));

        final String json = jsonObjectEnvelopeConverter.asJsonString(jsonEnvelope);

        return new EventBufferEvent(streamId,
                positionInStream,
                json,
                source,
                componentName,
                bufferedAt);
    }
}
