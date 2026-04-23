package uk.gov.justice.services.core.aggregate;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;

import uk.gov.justice.domain.aggregate.PrivateAggregate;
import uk.gov.justice.domain.aggregate.TestAggregate;
import uk.gov.justice.domain.event.EventA;
import uk.gov.justice.domain.event.EventB;
import uk.gov.justice.services.common.converter.JsonObjectToObjectConverter;
import uk.gov.justice.services.core.extension.EventFoundEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.eventsourcing.source.core.snapshot.async.AsyncSnapshotService;

import java.util.UUID;
import java.util.stream.Stream;

import jakarta.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

/**
 * Unit tests for the {@link DefaultAggregateService} class.
 */
@ExtendWith(MockitoExtension.class)
class DefaultAggregateServiceTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final long POSITION = 50001L;
    @Mock
    private Logger logger;

    @Mock
    private JsonObjectToObjectConverter jsonObjectToObjectConverter;

    @Mock
    private EventStream eventStream;

    @Mock
    private AsyncSnapshotService asyncSnapshotService;

    @InjectMocks
    private DefaultAggregateService aggregateService;

    private void registerEvent(Class clazz, String name) {
        aggregateService.register(new EventFoundEvent(clazz, name));
    }

    @Test
    void shouldCreateAggregateFromEmptyStream() {
        when(eventStream.read()).thenReturn(Stream.empty());
        when(eventStream.getId()).thenReturn(STREAM_ID);
        TestAggregate aggregate = aggregateService.get(eventStream, TestAggregate.class);

        assertThat(aggregate, notNullValue());
        assertThat(aggregate.recordedEvents(), empty());
        verify(logger).trace("Recreating aggregate for instance {} of aggregate type {}", STREAM_ID, TestAggregate.class);
    }

    @Test
    void shouldCreateAggregateFromSingletonStream() {
        JsonObject eventPayloadA = mock(JsonObject.class);
        EventA eventA = mock(EventA.class);
        when(jsonObjectToObjectConverter.convert(eventPayloadA, EventA.class)).thenReturn(eventA);
        when(eventStream.read()).thenReturn(Stream.of(
                envelopeFrom(
                        metadataBuilder()
                                .withStreamId(STREAM_ID)
                                .withId(randomUUID())
                                .withPosition(POSITION)
                                .withName("eventA"),
                        eventPayloadA)));
        when(eventStream.getId()).thenReturn(STREAM_ID);

        registerEvent(EventA.class, "eventA");

        TestAggregate aggregate = aggregateService.get(eventStream, TestAggregate.class);

        assertThat(aggregate, notNullValue());
        assertThat(aggregate.recordedEvents(), hasSize(1));
        assertThat(aggregate.recordedEvents().get(0), equalTo(eventA));
        verify(logger).info("Registering event {}, {} with DefaultAggregateService", "eventA", EventA.class);
        verify(logger).trace("Recreating aggregate for instance {} of aggregate type {}", STREAM_ID, TestAggregate.class);
        verify(asyncSnapshotService).saveAggregateSnapshot(STREAM_ID, POSITION - 1, aggregate);
        verifyNoMoreInteractions(asyncSnapshotService);
    }

    @Test
    void shouldCreateAggregateFromStreamOfTwo() {
        JsonObject eventPayloadA = mock(JsonObject.class);
        JsonObject eventPayloadB = mock(JsonObject.class);
        EventA eventA = mock(EventA.class);
        EventB eventB = mock(EventB.class);
        when(jsonObjectToObjectConverter.convert(eventPayloadA, EventA.class)).thenReturn(eventA);
        when(jsonObjectToObjectConverter.convert(eventPayloadB, EventB.class)).thenReturn(eventB);

        when(eventStream.read()).thenReturn(Stream.of(
                envelopeFrom(metadataBuilder()
                        .withStreamId(STREAM_ID)
                        .withId(randomUUID())
                        .withPosition(50001L)
                        .withName("eventA"), eventPayloadA),
                envelopeFrom(metadataBuilder()
                        .withStreamId(STREAM_ID)
                        .withId(randomUUID())
                        .withPosition(100001L)
                        .withName("eventB"), eventPayloadB)));
        when(eventStream.getId()).thenReturn(STREAM_ID);

        registerEvent(EventA.class, "eventA");
        registerEvent(EventB.class, "eventB");

        TestAggregate aggregate = aggregateService.get(eventStream, TestAggregate.class);

        assertThat(aggregate, notNullValue());
        assertThat(aggregate.recordedEvents(), hasSize(2));
        assertThat(aggregate.recordedEvents().get(0), equalTo(eventA));
        assertThat(aggregate.recordedEvents().get(1), equalTo(eventB));
        verify(logger).info("Registering event {}, {} with DefaultAggregateService", "eventA", EventA.class);
        verify(logger).info("Registering event {}, {} with DefaultAggregateService", "eventB", EventB.class);
        verify(logger).trace("Recreating aggregate for instance {} of aggregate type {}", STREAM_ID, TestAggregate.class);

        verify(asyncSnapshotService).saveAggregateSnapshot(STREAM_ID, 50001L - 1, aggregate);
        verify(asyncSnapshotService).saveAggregateSnapshot(STREAM_ID, 100001L - 1, aggregate);
        verifyNoMoreInteractions(asyncSnapshotService);
    }

    @Test
    void shouldCreateAggregateFromStreamOfThreeWithAFilteredOutSystemEvent() {
        JsonObject eventPayloadA = mock(JsonObject.class);
        JsonObject eventPayloadB = mock(JsonObject.class);

        EventA eventA = mock(EventA.class);
        EventB eventB = mock(EventB.class);
        when(jsonObjectToObjectConverter.convert(eventPayloadA, EventA.class)).thenReturn(eventA);
        when(jsonObjectToObjectConverter.convert(eventPayloadB, EventB.class)).thenReturn(eventB);
        when(eventStream.read()).thenReturn(Stream.of(
                envelopeFrom(metadataBuilder()
                        .withStreamId(STREAM_ID)
                        .withId(randomUUID())
                        .withName("eventA"), eventPayloadA),
                envelopeFrom(metadataBuilder()
                        .withStreamId(STREAM_ID)
                        .withId(randomUUID())
                        .withName("eventB"), eventPayloadB),
                envelopeFrom(metadataBuilder()
                        .withStreamId(STREAM_ID)
                        .withId(randomUUID())
                        .withName("system.events.eventC"), eventPayloadB)));

        when(eventStream.getId()).thenReturn(STREAM_ID);

        aggregateService.register(new EventFoundEvent(EventA.class, "eventA"));
        aggregateService.register(new EventFoundEvent(EventB.class, "eventB"));

        TestAggregate aggregate = aggregateService.get(eventStream, TestAggregate.class);

        assertThat(aggregate, notNullValue());
        assertThat(aggregate.recordedEvents(), hasSize(2));
        assertThat(aggregate.recordedEvents().get(0), equalTo(eventA));
        assertThat(aggregate.recordedEvents().get(1), equalTo(eventB));
        verify(logger).info("Registering event {}, {} with DefaultAggregateService", "eventA", EventA.class);
        verify(logger).info("Registering event {}, {} with DefaultAggregateService", "eventB", EventB.class);
        verify(logger).trace("Recreating aggregate for instance {} of aggregate type {}", STREAM_ID, TestAggregate.class);
    }

    @Test
    void shouldThrowExceptionForUnregisteredEvent() {
        when(eventStream.getId()).thenReturn(STREAM_ID);

        JsonObject eventPayloadA = mock(JsonObject.class);
        when(eventStream.read()).thenReturn(Stream.of(envelopeFrom(metadataBuilder()
                .withStreamId(STREAM_ID)
                .withId(randomUUID())
                .withName("eventA"), eventPayloadA)));

        assertThrows(IllegalStateException.class, () -> aggregateService.get(eventStream, TestAggregate.class));
    }

    @Test
    void shouldThrowExceptionForNonInstantiatableEvent() {

        registerEvent(EventA.class, "eventA");

        assertThrows(RuntimeException.class, () -> aggregateService.get(eventStream, PrivateAggregate.class));
    }

    @Test
    void shouldNeedToSaveSnapshotInBackground() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(100001L, randomUUID());
        assertThat(actual, equalTo(true));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfPositionInStreamModulo50000IsNot1() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(50000L, randomUUID());
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfPositionInStreamIsNotGreaterThanZero() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(-50001L, randomUUID());
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfPositionInStreamIsZero() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(0L, randomUUID());
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfPositionInStreamIsOne() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(1L, randomUUID());
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfNoPositionInStream() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(null, randomUUID());
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundIfNoStreamId() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(null, null);
        assertThat(actual, equalTo(false));
    }

    @Test
    void shouldNotNeedToSaveSnapshotInBackgroundWhenPositionIsNotPositive() {
        final boolean actual = aggregateService.needsToSaveSnapshotInBackground(0L, randomUUID());
        assertThat(actual, equalTo(false));
    }

}