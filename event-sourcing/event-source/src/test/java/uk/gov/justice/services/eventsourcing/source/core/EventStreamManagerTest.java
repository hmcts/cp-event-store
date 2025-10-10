package uk.gov.justice.services.eventsourcing.source.core;

import static java.util.Optional.empty;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.jsonBuilderFactory;
import static uk.gov.justice.services.test.utils.core.enveloper.EnveloperFactory.createEnveloper;

import uk.gov.justice.services.core.enveloper.Enveloper;
import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcBasedEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.OptimisticLockingRetryException;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.eventsourcing.source.core.exception.VersionMismatchException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventStreamManagerTest {

    private static final UUID STREAM_ID = randomUUID();
    private static final Long INITIAL_VERSION = 0L;
    private static final Long CURRENT_VERSION = 5L;
    private static final Long INVALID_VERSION = 8L;
    private static final String EVENT_SOURCE_NAME = "eventSourceName";

    private static final long MAX_RETRY = 23L;

    @Mock
    private JdbcBasedEventRepository eventRepository;

    @Mock
    private PublishingEventAppender publishingEventAppender;

    @SuppressWarnings("unchecked")
    @Mock
    private Stream<JsonEnvelope> eventStream;

    @Mock
    private SystemEventService systemEventService;

    @SuppressWarnings("unused")
    @Spy
    private Enveloper enveloper = createEnveloper();

    @Mock
    private EventSourceNameProvider eventSourceNameProvider;

    @Mock
    private MaxRetryProvider maxRetryProvider;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventStreamManager eventStreamManager;

    @Captor
    private ArgumentCaptor<JsonEnvelope> eventCaptor;

    @Captor
    private ArgumentCaptor<Long> versionCaptor;

    @Test
    public void shouldAppendToStream() throws Exception {

        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        eventStreamManager.append(STREAM_ID, Stream.of(event));

        verify(publishingEventAppender).append(event, STREAM_ID, INITIAL_VERSION + 1, EVENT_SOURCE_NAME);

    }

    @Test
    public void shouldThrowExceptionWhenEnvelopeContainsVersion() throws Exception {
        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event").withVersion(INITIAL_VERSION + 1),
                jsonBuilderFactory.createObjectBuilder());
        assertThrows(EventStreamException.class, () -> eventStreamManager.append(STREAM_ID, Stream.of(event)));
    }

    @Test
    public void shouldAppendToStreamFromVersion() throws Exception {

        final long expectedVersion = CURRENT_VERSION + 1;

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(CURRENT_VERSION);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        eventStreamManager.appendAfter(STREAM_ID, Stream.of(event), CURRENT_VERSION);


        verify(publishingEventAppender).append(event, STREAM_ID, expectedVersion, EVENT_SOURCE_NAME);
    }

    @Test
    public void appendToStreamShouldReturnCurrentVersion() throws Exception {

        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(6L);

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        long returnedVersion = eventStreamManager.append(STREAM_ID, Stream.of(event));

        assertThat(returnedVersion, is(7L));

    }

    @Test
    public void appendAfterShouldReturnCurrentVersion() throws Exception {

        final long currentVersion = 4L;
        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(currentVersion);

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        long returnedVersion = eventStreamManager.appendAfter(
                STREAM_ID,
                Stream.of(event),
                currentVersion);

        assertThat(returnedVersion, is(currentVersion + 1));

    }

    @Test
    public void shouldThrowExceptionOnNullFromVersion() throws Exception {
        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());
        assertThrows(EventStreamException.class, () -> eventStreamManager.appendAfter(STREAM_ID, Stream.of(event), null));
    }

    @Test
    public void shouldThrowExceptionWhenEventsAreMissing() throws Exception {

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        assertThrows(VersionMismatchException.class, () -> eventStreamManager.appendAfter(STREAM_ID, Stream.of(event), INVALID_VERSION));
    }

    @Test
    public void shouldThrowExceptionWhenVersionAlreadyExists() throws Exception {
        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(CURRENT_VERSION + 1);

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        assertThrows(OptimisticLockingRetryException.class, () -> eventStreamManager.appendAfter(STREAM_ID, Stream.of(event), CURRENT_VERSION));
    }

    @Test
    public void shouldReadStream() {
        when(eventRepository.getEventsByStreamId(STREAM_ID)).thenReturn(eventStream);

        final Stream<JsonEnvelope> actualEnvelopeEventStream = eventStreamManager.read(STREAM_ID);

        assertThat(actualEnvelopeEventStream, equalTo(eventStream));
        verify(eventRepository).getEventsByStreamId(STREAM_ID);
    }

    @Test
    public void shouldReadStreamFromVersion() {
        when(eventRepository.getEventsByStreamIdFromPosition(STREAM_ID, CURRENT_VERSION)).thenReturn(eventStream);

        final Stream<JsonEnvelope> actualEnvelopeEventStream = eventStreamManager.readFrom(STREAM_ID, CURRENT_VERSION);

        assertThat(actualEnvelopeEventStream, equalTo(eventStream));
        verify(eventRepository).getEventsByStreamIdFromPosition(STREAM_ID, CURRENT_VERSION);
    }

    @Test
    public void shouldReadStreamFromVersionByPage() {

        final int pageSize = 1000;

        when(eventRepository.getEventsByStreamIdFromPosition(STREAM_ID, CURRENT_VERSION, pageSize)).thenReturn(eventStream);
        final Stream<JsonEnvelope> actualEnvelopeEventStream = eventStreamManager.readFrom(STREAM_ID, CURRENT_VERSION, pageSize);

        assertThat(actualEnvelopeEventStream, equalTo(eventStream));
        verify(eventRepository).getEventsByStreamIdFromPosition(STREAM_ID, CURRENT_VERSION, pageSize);
    }

    @Test
    public void shouldGetCurrentVersion() {
        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(CURRENT_VERSION);
        final Long actualCurrentVersion = eventStreamManager.getSize(STREAM_ID);

        assertThat(actualCurrentVersion, equalTo(CURRENT_VERSION));
        verify(eventRepository).getStreamSize(STREAM_ID);
    }

    @Test
    public void shouldAppendNonConsecutively() throws Exception {

        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(CURRENT_VERSION);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-1"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-2"),
                jsonBuilderFactory.createObjectBuilder());

        eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event1, event2));

        verify(publishingEventAppender).append(event1, STREAM_ID, CURRENT_VERSION + 1, EVENT_SOURCE_NAME);
        verify(publishingEventAppender).append(event2, STREAM_ID, CURRENT_VERSION + 2, EVENT_SOURCE_NAME);

    }

    @Test
    public void shouldReturnCurrentVersionWhenAppendingNonConsecutively() throws Exception {
        when(eventRepository.getStreamSize(STREAM_ID)).thenReturn(CURRENT_VERSION);

        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-1"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-2"),
                jsonBuilderFactory.createObjectBuilder());

        long returnedVersion = eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event1, event2));
        assertThat(returnedVersion, is(CURRENT_VERSION + 2));

    }

    @Test
    public void shouldRetryWithNextVersionIdInCaseOfOptimisticLockException() throws Exception {

        final long currentVersion = 6L;
        final long currentVersionAfterException = 11L;

        when(eventRepository.getStreamSize(STREAM_ID))
                .thenReturn(currentVersion).thenReturn(currentVersionAfterException);
        when(maxRetryProvider.getMaxRetry()).thenReturn(20L);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);


        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-1"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-2"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event3 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-3"),
                jsonBuilderFactory.createObjectBuilder());

        doNothing().when(publishingEventAppender).append(eq(event1), eq(STREAM_ID), anyLong(), eq(EVENT_SOURCE_NAME));
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(eq(event2), eq(STREAM_ID), eq(currentVersion + 2), eq(EVENT_SOURCE_NAME));
        doNothing().when(publishingEventAppender).append(eq(event2), eq(STREAM_ID), eq(currentVersionAfterException + 1), eq(EVENT_SOURCE_NAME));
        doNothing().when(publishingEventAppender).append(eq(event3), eq(STREAM_ID), anyLong(), eq(EVENT_SOURCE_NAME));

        eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event1, event2, event3));

        verify(publishingEventAppender).append(event1, STREAM_ID, currentVersion + 1, EVENT_SOURCE_NAME);
        verify(publishingEventAppender).append(event2, STREAM_ID, currentVersion + 2, EVENT_SOURCE_NAME);
        verify(publishingEventAppender).append(event2, STREAM_ID, currentVersionAfterException + 1, EVENT_SOURCE_NAME);
        verify(publishingEventAppender).append(event3, STREAM_ID, currentVersionAfterException + 2, EVENT_SOURCE_NAME);
    }

    @Test
    public void shouldTraceLogAnAttemptedRetry() throws Exception {

        final long currentVersion = 6L;
        final long currentVersionAfterException = 11L;

        when(eventRepository.getStreamSize(STREAM_ID))
                .thenReturn(currentVersion).thenReturn(currentVersionAfterException);
        when(maxRetryProvider.getMaxRetry()).thenReturn(20L);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-1"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-2"),
                jsonBuilderFactory.createObjectBuilder());
        final JsonEnvelope event3 = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event-3"),
                jsonBuilderFactory.createObjectBuilder());

        doNothing().when(publishingEventAppender).append(eq(event1), eq(STREAM_ID), anyLong(), eq(EVENT_SOURCE_NAME));
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(eq(event2), eq(STREAM_ID), eq(currentVersion + 2), eq(EVENT_SOURCE_NAME));
        doNothing().when(publishingEventAppender).append(eq(event2), eq(STREAM_ID), eq(currentVersionAfterException + 1), eq(EVENT_SOURCE_NAME));
        doNothing().when(publishingEventAppender).append(eq(event3), eq(STREAM_ID), anyLong(), eq(EVENT_SOURCE_NAME));

        eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event1, event2, event3));

        verify(logger).trace("Retrying appending to stream {}, with version {}", STREAM_ID, currentVersionAfterException + 1);
    }

    @Test
    public void shouldThrowExceptionAfterMaxNumberOfRetriesReached() throws Exception {

        final long currentVersion = 6L;
        final long currentVersionAfterException1 = 11L;
        final long currentVersionAfterException2 = 12L;

        when(eventRepository.getStreamSize(STREAM_ID))
                .thenReturn(currentVersion)
                .thenReturn(currentVersionAfterException1)
                .thenReturn(currentVersionAfterException2);
        when(maxRetryProvider.getMaxRetry()).thenReturn(2L);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersion + 1, EVENT_SOURCE_NAME);
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersionAfterException1 + 1, EVENT_SOURCE_NAME);
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersionAfterException2 + 1, EVENT_SOURCE_NAME);


        assertThrows(OptimisticLockingRetryException.class, () ->
                eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event))
        );
    }

    @Test
    public void shouldLogWarningAfterMaxNumberOfRetriesReached() throws Exception {

        final long currentVersion = 6L;
        final long currentVersionAfterException1 = 11L;
        final long currentVersionAfterException2 = 12L;

        when(eventRepository.getStreamSize(STREAM_ID))
                .thenReturn(currentVersion)
                .thenReturn(currentVersionAfterException1)
                .thenReturn(currentVersionAfterException2);
        when(maxRetryProvider.getMaxRetry()).thenReturn(2L);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);


        final JsonEnvelope event = envelopeFrom(
                metadataBuilder().withId(randomUUID()).withName("my-event"),
                jsonBuilderFactory.createObjectBuilder());

        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersion + 1, EVENT_SOURCE_NAME);
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersionAfterException1 + 1, EVENT_SOURCE_NAME);
        doThrow(OptimisticLockingRetryException.class).when(publishingEventAppender).append(event, STREAM_ID, currentVersionAfterException2 + 1, EVENT_SOURCE_NAME);

        try {
            eventStreamManager.appendNonConsecutively(STREAM_ID, Stream.of(event));
        } catch (final OptimisticLockingRetryException e) {
        }

        verify(logger).warn("Failed to append to stream {} due to concurrency issues, returning to handler.", STREAM_ID);
    }

    @Test
    public void shouldCloneStreamWithBlankVersions() throws EventStreamException {

        final JsonEnvelope event = buildEnvelope("test.events.event1");
        final JsonEnvelope systemEvent = buildEnvelope("system.events.cloned");

        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);
        when(eventRepository.getEventsByStreamId(STREAM_ID)).thenReturn(Stream.of(event));
        when(systemEventService.clonedEventFor(STREAM_ID)).thenReturn(systemEvent);

        final UUID clonedId = eventStreamManager.cloneAsAncestor(STREAM_ID);

        assertThat(clonedId, is(notNullValue()));
        assertThat(clonedId, is(not(STREAM_ID)));

        verify(publishingEventAppender, times(2)).append(eventCaptor.capture(), eq(clonedId), versionCaptor.capture(), eq(EVENT_SOURCE_NAME));
        final List<JsonEnvelope> clonedEvents = eventCaptor.getAllValues();

        assertThat(versionCaptor.getAllValues(), hasItems(1L, 2L));
        assertThat(clonedEvents, hasItems(systemEvent));
        final JsonEnvelope clonedEvent = clonedEvents.get(0);
        assertThat(clonedEvent.metadata().name(), is("test.events.event1"));
        assertThat(clonedEvent.metadata().position(), is(empty()));

        verify(eventRepository).markEventStreamActive(clonedId, false);
    }

    @Test
    public void shouldCloneAllEventsOnAStream() throws EventStreamException {

        final JsonEnvelope event1 = buildEnvelope("test.events.event1");
        final JsonEnvelope event2 = buildEnvelope("test.events.event2");
        final JsonEnvelope systemEvent = buildEnvelope("system.events.cloned");

        when(eventRepository.getEventsByStreamId(STREAM_ID)).thenReturn(Stream.of(event1, event2));
        when(systemEventService.clonedEventFor(STREAM_ID)).thenReturn(systemEvent);
        when(eventSourceNameProvider.getDefaultEventSourceName()).thenReturn(EVENT_SOURCE_NAME);

        final UUID clonedId = eventStreamManager.cloneAsAncestor(STREAM_ID);

        assertThat(clonedId, is(notNullValue()));
        assertThat(clonedId, is(not(STREAM_ID)));

        verify(publishingEventAppender, times(3)).append(eventCaptor.capture(), eq(clonedId), versionCaptor.capture(), eq(EVENT_SOURCE_NAME));
        assertThat(versionCaptor.getAllValues(), hasItems(1L, 2L, 3L));

        verify(eventRepository).markEventStreamActive(clonedId, false);
    }

    @Test
    public void shouldClearEventStream() throws Exception {
        eventStreamManager.clear(STREAM_ID);

        verify(eventRepository).clearEventsForStream(STREAM_ID);
        verifyNoMoreInteractions(eventRepository, publishingEventAppender);
    }

    @Test
    public void shouldGetTheStreamPositionFromTheEventStreamManager() throws Exception {

        final UUID streamId = randomUUID();
        final long streamPosition = 23L;

        when(eventRepository.getStreamPosition(streamId)).thenReturn(streamPosition);

        assertThat(eventStreamManager.getStreamPosition(streamId), is(streamPosition));
    }

    private JsonEnvelope buildEnvelope(final String eventName) {
        return envelopeFrom(
                metadataBuilder().withId(randomUUID()).withStreamId(STREAM_ID).withName(eventName),
                jsonBuilderFactory.createObjectBuilder().add("field", "value").build());
    }
}
