package uk.gov.justice.services.event.buffer.core.service;

import static co.unruly.matchers.StreamMatchers.contains;
import static co.unruly.matchers.StreamMatchers.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.jsonBuilderFactory;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferJdbcRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusJdbcRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.Subscription;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.test.utils.common.stream.StreamCloseSpy;

import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ConsecutiveEventBufferServiceTest {

    public static final String EVENT_LISTENER = "EVENT_LISTENER";
    @Mock
    @SuppressWarnings("unused")
    private Logger logger;

    @Mock
    private StreamStatusJdbcRepository streamStatusJdbcRepository;

    @Mock
    private EventBufferJdbcRepository streamBufferRepository;

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private ConsecutiveEventBufferService bufferService;


    @Test
    public void shouldThrowExceptionIfNoStreamId() {


        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event").withPosition(1L),
                jsonBuilderFactory.createObjectBuilder()
        );

        assertThrows(IllegalStateException.class, () -> bufferService.currentOrderedEventsWith(event, EVENT_LISTENER));
    }

    @Test
    public void shouldNotAllowZeroVersion() {

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event").withPosition(0L),
                jsonBuilderFactory.createObjectBuilder()
        );

        assertThrows(IllegalStateException.class, () -> bufferService.currentOrderedEventsWith(event, EVENT_LISTENER));
    }

    @Test
    public void shouldNotAllowNullVersion() {

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder()
        );
        assertThrows(IllegalStateException.class, () -> bufferService.currentOrderedEventsWith(event, EVENT_LISTENER));
    }

    @Test
    public void shouldIgnoreObsoleteEvent() {

        final UUID streamId = randomUUID();
        final String source = "source";
        final String eventName = "source.event.name";

        final JsonEnvelope event_3 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(3L),
                jsonBuilderFactory.createObjectBuilder()
        );
        final JsonEnvelope event_4 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(4L),
                jsonBuilderFactory.createObjectBuilder()
        );

        final Subscription subscription = mock(Subscription.class);

        when(eventSourceNameCalculator.getSource(event_3)).thenReturn(source);
        when(eventSourceNameCalculator.getSource(event_4)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, EVENT_LISTENER)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(4L);


        assertThat(bufferService.currentOrderedEventsWith(event_3, EVENT_LISTENER), is(empty()));
        assertThat(bufferService.currentOrderedEventsWith(event_4, EVENT_LISTENER), is(empty()));

        verifyNoInteractions(streamBufferRepository);

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);
        
        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, EVENT_LISTENER);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, EVENT_LISTENER));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, EVENT_LISTENER);

    }

    @Test
    public void shouldReturnEventThatIsInCorrectOrder() {
        final UUID streamId = randomUUID();
        final String source = "source";
        final String eventName = "source.event.name";
        final String component = EVENT_LISTENER;

        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(5L),
                jsonBuilderFactory.createObjectBuilder()
        );

        final Subscription subscription = mock(Subscription.class);

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(4L);

        when(streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component)).thenReturn(Stream.empty());

        final Stream<JsonEnvelope> returnedEvents = bufferService.currentOrderedEventsWith(incomingEvent, component);
        assertThat(returnedEvents, contains(incomingEvent));

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, component);

    }

    @Test
    public void shouldIncrementVersionOnIncomingEventInCorrectOrder() {
        final UUID streamId = UUID.fromString("8d104aa3-6ea5-4569-8730-4231e5faaaaa");

        final String source = "source";
        final String component = EVENT_LISTENER;
        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("source.event-name").withSource(source).withStreamId(streamId).withPosition(5L),
                jsonBuilderFactory.createObjectBuilder()
        );

        final Subscription subscription = mock(Subscription.class);

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(4L);
        when(streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component)).thenReturn(Stream.empty());

        bufferService.currentOrderedEventsWith(incomingEvent, component);

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).update(new Subscription(streamId, 5L, source, component));

    }

    @Test
    public void shouldStoreEventIncomingNotInOrderAndReturnEmpty() {
        final String eventName = "source.events.something.happened";
        final String source = "source";
        final UUID streamId = randomUUID();
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(6L),
                jsonBuilderFactory.createObjectBuilder()
        );

        final Subscription subscription = mock(Subscription.class);

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, EVENT_LISTENER)).thenReturn(of(subscription));
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, EVENT_LISTENER)).thenReturn(of(new Subscription(streamId, 4L, source, EVENT_LISTENER)));
        when(clock.now()).thenReturn(bufferedAt);

        when(jsonObjectEnvelopeConverter.asJsonString(incomingEvent)).thenReturn("someStringRepresentation");

        final Stream<JsonEnvelope> returnedEvents = bufferService.currentOrderedEventsWith(incomingEvent, EVENT_LISTENER);

        final InOrder inOrder = inOrder(streamStatusJdbcRepository, streamBufferRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, EVENT_LISTENER);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, EVENT_LISTENER));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, EVENT_LISTENER);
        inOrder.verify(streamBufferRepository).insert(new EventBufferEvent(streamId, 6L, "someStringRepresentation", source, EVENT_LISTENER, bufferedAt));

        assertThat(returnedEvents, empty());
    }

    @Test
    public void shouldReturnConsecutiveBufferedEventsIfIncomingEventFillsTheVersionGap() {

        final UUID streamId = randomUUID();
        final String source = "source";
        final String eventName = "source.event.name";
        final String component = EVENT_LISTENER;
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(3L),
                jsonBuilderFactory.createObjectBuilder()
        );

        final Subscription subscription = mock(Subscription.class);

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(2L);

        when(streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component)).thenReturn(
                Stream.of(new EventBufferEvent(streamId, 4L, "someEventContent4", "source_4", component, bufferedAt),
                        new EventBufferEvent(streamId, 5L, "someEventContent5", "source_5", component, bufferedAt),
                        new EventBufferEvent(streamId, 6L, "someEventContent6", "source_6", component, bufferedAt),
                        new EventBufferEvent(streamId, 8L, "someEventContent8", "source_8", component, bufferedAt),
                        new EventBufferEvent(streamId, 9L, "someEventContent9", "source_9", component, bufferedAt),
                        new EventBufferEvent(streamId, 10L, "someEventContent10", "source_10", component, bufferedAt),
                        new EventBufferEvent(streamId, 11L, "someEventContent11", "source_11", component, bufferedAt)));

        final JsonEnvelope bufferedEvent4 = mock(JsonEnvelope.class);
        final JsonEnvelope bufferedEvent5 = mock(JsonEnvelope.class);
        final JsonEnvelope bufferedEvent6 = mock(JsonEnvelope.class);

        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent4")).thenReturn(bufferedEvent4);
        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent5")).thenReturn(bufferedEvent5);
        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent6")).thenReturn(bufferedEvent6);

        final Stream<JsonEnvelope> returnedEvents = bufferService.currentOrderedEventsWith(incomingEvent, component);
        assertThat(returnedEvents, contains(incomingEvent, bufferedEvent4, bufferedEvent5, bufferedEvent6));

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, component);

    }

    @Test
    public void shouldCloseSourceStreamOnConsecutiveStreamClose() {

        final UUID streamId = randomUUID();
        final String source = "source";
        final String eventName = "source.event.name";
        final String component = EVENT_LISTENER;
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final Subscription subscription = mock(Subscription.class);

        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(3L),
                jsonBuilderFactory.createObjectBuilder()
        );

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(2L);

        final StreamCloseSpy sourceStreamSpy = new StreamCloseSpy();

        when(streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component)).thenReturn(
                Stream.of(new EventBufferEvent(streamId, 4L, "someEventContent4", source, component, bufferedAt),
                        new EventBufferEvent(streamId, 8L, "someEventContent8", source, component, bufferedAt)).onClose(sourceStreamSpy)
        );

        final Stream<JsonEnvelope> returnedEvents = bufferService.currentOrderedEventsWith(incomingEvent, component);
        returnedEvents.close();

        assertThat(sourceStreamSpy.streamClosed(), is(true));

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, component);
    }

    @Test
    public void shouldRemoveEventsFromBufferOnceStreamed() {

        final UUID streamId = randomUUID();
        final String source = "source";
        final String eventName = "source.event.name";
        final String component = EVENT_LISTENER;
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final Subscription subscription = mock(Subscription.class);

        when(streamStatusJdbcRepository.findByStreamIdAndSource(streamId, source, component)).thenReturn(of(subscription));
        when(subscription.getPosition()).thenReturn(2L);

        final EventBufferEvent event4 = new EventBufferEvent(streamId, 4L, "someEventContent4", "source_1", component, bufferedAt);
        final EventBufferEvent event5 = new EventBufferEvent(streamId, 5L, "someEventContent5", "source_2", component, bufferedAt);
        final EventBufferEvent event6 = new EventBufferEvent(streamId, 6L, "someEventContent6", "source_3", component, bufferedAt);

        final JsonEnvelope bufferedEvent4 = mock(JsonEnvelope.class);
        final JsonEnvelope bufferedEvent5 = mock(JsonEnvelope.class);
        final JsonEnvelope bufferedEvent6 = mock(JsonEnvelope.class);

        final JsonEnvelope incomingEvent = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName(eventName).withStreamId(streamId).withPosition(3L),
                jsonBuilderFactory.createObjectBuilder()
        );

        when(eventSourceNameCalculator.getSource(incomingEvent)).thenReturn(source);
        when(streamBufferRepository.findStreamByIdSourceAndComponent(streamId, source, component)).thenReturn(
                Stream.of(event4, event5, event6));
        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent4")).thenReturn(bufferedEvent4);
        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent5")).thenReturn(bufferedEvent5);
        when(jsonObjectEnvelopeConverter.asEnvelope("someEventContent6")).thenReturn(bufferedEvent6);

        final Stream<JsonEnvelope> returnedEvents = bufferService.currentOrderedEventsWith(incomingEvent, component);

        assertThat(returnedEvents, contains(incomingEvent, bufferedEvent4, bufferedEvent5, bufferedEvent6));

        final InOrder inOrder = inOrder(streamStatusJdbcRepository);

        inOrder.verify(streamStatusJdbcRepository).updateSource(streamId, source, component);
        inOrder.verify(streamStatusJdbcRepository).insertOrDoNothing(new Subscription(streamId, 0L, source, component));
        inOrder.verify(streamStatusJdbcRepository).findByStreamIdAndSource(streamId, source, component);

        verify(streamBufferRepository).remove(event4);
        verify(streamBufferRepository).remove(event5);
        verify(streamBufferRepository).remove(event6);
    }
}
