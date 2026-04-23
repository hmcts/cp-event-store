package uk.gov.justice.services.event.buffer.core.service;

import static java.lang.String.format;
import static java.util.stream.Stream.concat;
import static java.util.stream.StreamSupport.stream;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferJdbcRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusJdbcRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.Subscription;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.util.UUID;
import java.util.stream.Stream;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;

import org.slf4j.Logger;

@ApplicationScoped
@Alternative
@Priority(2)
public class ConsecutiveEventBufferService implements EventBufferService {

    static final long INITIAL_POSITION = 1L;

    @Inject
    private Logger logger;

    @Inject
    private EventBufferJdbcRepository streamBufferRepository;

    @Inject
    private StreamStatusJdbcRepository streamStatusJdbcRepository;

    @Inject
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Inject
    private UtcClock clock;

    /**
     * Takes an incoming event and returns a stream of json envelopes. If the event is not
     * consecutive according to the stream_status repository then an empty stream is returned and
     * the incomingEvent is added to the event-buffer-repository. If the incomingEvent is
     * consecutive then it is returned as a stream with any consecutive events from the buffer. If
     * an event is the first to be processed for that streamId then the version value must be 1 or
     * the incomingEvent is added to the buffer and an empty stream is returned.
     *
     * @return stream of consecutive events
     */
    @Override
    public Stream<JsonEnvelope> currentOrderedEventsWith(final JsonEnvelope incomingEvent, final String component) {

        logger.trace("Message buffering for message: {}", incomingEvent);

        final Metadata metadata = incomingEvent.metadata();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new IllegalStateException("Event must have a a streamId "));
        final long incomingEventVersion = versionOf(incomingEvent);
        final String source = eventSourceNameCalculator.getSource(incomingEvent);

        streamStatusJdbcRepository.updateSource(streamId, source, component);
        streamStatusJdbcRepository.insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        final long currentVersion = streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)
                .orElseThrow(() -> new IllegalStateException("stream status cannot be empty"))
                .getPosition();

        if (incomingEventObsolete(incomingEventVersion, currentVersion)) {
            logger.warn(format("Obsolete EventBuffer message: id = '%s', streamId = '%s', name = '%s', source = '%s', eventNumber = %s, component = '%s'",
                    metadata.id(),
                    metadata.streamId(),
                    metadata.name(),
                    metadata.source(),
                    metadata.eventNumber().orElse(null),
                    component));
            return Stream.empty();

        } else if (incomingEventNotInOrder(incomingEventVersion, currentVersion)) {
            logger.trace("Message : {} is not consecutive, adding to buffer", incomingEvent);
            addToBuffer(incomingEvent, streamId, incomingEventVersion, component, source);
            return Stream.empty();

        } else {
            logger.trace("Message : {} version is valid sending stream to dispatcher", incomingEvent);
            streamStatusJdbcRepository.update(new Subscription(streamId, incomingEventVersion, source, component));
            return bufferedEvents(streamId, incomingEvent, incomingEventVersion, component, source);
        }
    }

    private long versionOf(final JsonEnvelope event) {
        final long incomingEventVersion = event.metadata().position().orElseThrow(() -> new IllegalStateException("Event must have a version"));

        if (incomingEventVersion < INITIAL_POSITION) {
            throw new IllegalStateException("Version cannot be less than " + INITIAL_POSITION);
        }
        
        return incomingEventVersion;
    }

    private Stream<JsonEnvelope> bufferedEvents(
            final UUID streamId,
            final JsonEnvelope incomingEvent,
            final long incomingEventVersion,
            final String component,
            final String source) {
        final Stream<EventBufferEvent> stream = streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component);
        return concat(Stream.of(incomingEvent), consecutiveEventStreamFromBuffer(stream, incomingEventVersion)
                .peek(streamBufferEvent -> streamBufferRepository.remove(streamBufferEvent))
                .peek(streamBufferEvent -> streamStatusJdbcRepository.update(new Subscription(
                        streamBufferEvent.getStreamId(),
                        streamBufferEvent.getPosition(),
                        source,
                        component)))
                .map(streamBufferEvent -> jsonObjectEnvelopeConverter.asEnvelope(streamBufferEvent.getEvent())));
    }

    private void addToBuffer(
            final JsonEnvelope incomingEvent,
            final UUID streamId,
            final Long incomingEventVersion,
            final String component,
            final String source) {
        streamBufferRepository.insert(
                new EventBufferEvent(streamId,
                        incomingEventVersion,
                        jsonObjectEnvelopeConverter.asJsonString(incomingEvent),
                        source,
                        component,
                        clock.now()));

    }

    private Stream<EventBufferEvent> consecutiveEventStreamFromBuffer(final Stream<EventBufferEvent> messageBuffer, final long currentVersion) {
        return stream(new ConsecutiveEventsSpliterator(messageBuffer, currentVersion), false).onClose(messageBuffer::close);
    }

    private boolean incomingEventNotInOrder(final long incomingEventVersion, final long currentVersion) {
        return incomingEventVersion - currentVersion > 1;
    }

    private boolean incomingEventObsolete(final long incomingEventVersion, final long currentVersion) {
        return incomingEventVersion - currentVersion <= 0;
    }
}
