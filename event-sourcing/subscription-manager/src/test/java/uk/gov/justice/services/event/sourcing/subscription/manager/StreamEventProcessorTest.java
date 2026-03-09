package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamEventProcessorTest {

    @Mock
    private ComponentEventProcessor componentEventProcessor;

    @Mock
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Mock
    private NextEventSelector nextEventSelector;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private StreamProcessingConfig streamProcessingConfig;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Mock
    private StreamEventValidator streamEventValidator;

    @Mock
    private StreamSessionLockManager streamSessionLockManager;

    @InjectMocks
    private StreamEventProcessor streamEventProcessor;

    @Test
    public void shouldProcessEventAndMarkStreamAsUpToDate() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 6L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                newStreamStatusRepository,
                nextEventSelector,
                streamSessionLockManager,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                componentEventProcessor,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin();
        inOrder.verify(newStreamStatusRepository).findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any());
        inOrder.verify(nextEventSelector).selectNextEvent(source, of(lockedStreamStatus));
        inOrder.verify(streamSessionLockManager).lockStream(streamId, source, component);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(componentEventProcessor).process(eventJsonEnvelope, component);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingSuccess(streamId, source, component, empty());
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit();

        verify(lock).close();
        verify(transactionHandler, never()).rollback();
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldProcessEventAndNotMarkStreamAsUpToDateWhenPositionNotEqualToLatestKnownPosition() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(streamErrorStatusHandler).onStreamProcessingSuccess(streamId, source, component, empty());
        verify(transactionHandler, never()).rollback();
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(lock).close();
    }

    @Test
    public void shouldReturnEventNotFoundWhenNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(empty());
        when(nextEventSelector.selectNextEvent(source, empty())).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                newStreamStatusRepository,
                nextEventSelector);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin();
        inOrder.verify(newStreamStatusRepository).findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any());
        inOrder.verify(nextEventSelector).selectNextEvent(source, empty());
        inOrder.verify(transactionHandler).rollback();

        verifyNoInteractions(componentEventProcessor);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
    }

    @Test
    public void shouldRethrowStreamProcessingExceptionAndRollbackWhenStreamSelectionFails() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final StreamProcessingException streamProcessingException = new StreamProcessingException("Stream selection failed");

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown, is(streamProcessingException));

        verify(transactionHandler).rollback();
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
        verifyNoInteractions(nextEventSelector);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRollbackWhenEventNotFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition));

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        verify(transactionHandler).rollback();
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
    }

    @Test
    public void shouldReturnEventFoundAndRecordErrorIfEventProcessingFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, of(streamErrorId));
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(true);
        doThrow(nullPointerException).when(componentEventProcessor).process(eventJsonEnvelope, component);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(transactionHandler).rollback();
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, component, currentPosition, of(streamErrorId));

        verify(lock).close();
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(transactionHandler, never()).commit();
    }

    @Test
    public void shouldReturnEventFoundAndRecordErrorIfNoPositionFoundInEvent() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(transactionHandler).rollback();
        verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler).onStreamProcessingFailure(eq(eventJsonEnvelope), any(MissingPositionInStreamException.class), eq(component), eq(currentPosition), eq(empty()));
        verify(componentEventProcessor).process(eventJsonEnvelope, component);
        verify(lock).close();
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenFindingEventFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition));

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        verify(transactionHandler).rollback();
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
    }

    @Test
    public void shouldWrapGenericExceptionInStreamProcessingExceptionWhenSelectEventFails() {

        final String source = "some-source";
        final String component = "some-component";
        final RuntimeException genericException = new RuntimeException("Database connection lost");

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenThrow(genericException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Failed to select event for processing: source 'some-source', component 'some-component'"));
        assertThat(thrown.getCause(), is(genericException));

        verify(transactionHandler).rollback();
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
        verifyNoInteractions(componentEventProcessor);
    }

    @Test
    public void shouldWrapGenericExceptionFromNextEventSelectorInStreamProcessingException() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final IllegalStateException genericException = new IllegalStateException("Unexpected state");

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenThrow(genericException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Failed to select event for processing: source 'some-source', component 'some-component'"));
        assertThat(thrown.getCause(), is(genericException));

        verify(transactionHandler).rollback();
        verify(streamSessionLockManager, never()).lockStream(any(), any(), any());
        verifyNoInteractions(componentEventProcessor);
    }

    @Test
    public void shouldReturnEventFoundWhenAdvisoryLockCannotBeAcquired() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(false);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verifyNoInteractions(componentEventProcessor);
        verify(transactionHandler).rollback();
        verify(lock).close();
    }

    @Test
    public void shouldCallOnStreamProcessingSuccessWithStreamErrorIdWhenProcessingSucceeds() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 6L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, of(streamErrorId));
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final StreamSessionLockManager.StreamSessionLock lock = mock(StreamSessionLockManager.StreamSessionLock.class);

        when(newStreamStatusRepository.findOldestStreamToProcessByAcquiringLock(eq(source), eq(component), any())).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.lockStream(streamId, source, component)).thenReturn(lock);
        when(lock.isAcquired()).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(streamErrorStatusHandler).onStreamProcessingSuccess(streamId, source, component, of(streamErrorId));
        verify(lock).close();
        verify(transactionHandler, never()).rollback();
    }
}