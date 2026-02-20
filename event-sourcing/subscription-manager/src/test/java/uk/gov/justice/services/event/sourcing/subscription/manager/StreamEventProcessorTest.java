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
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamRetryStatusManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.sql.Connection;
import java.util.UUID;

import javax.transaction.UserTransaction;

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
    private StreamSelectorManager streamSelectorManager;

    @Mock
    private NextEventSelector nextEventSelector;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Mock
    private StreamEventValidator streamEventValidator;

    @Mock
    private StreamRetryStatusManager streamRetryStatusManager;

    @Mock
    private StreamErrorRepository streamErrorRepository;

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

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                streamSessionLockManager,
                transactionHandler,
                streamSelectorManager,
                nextEventSelector,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                componentEventProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler,
                streamRetryStatusManager);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(streamSessionLockManager).openLockConnection();
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelectorManager).selectStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, of(lockedStreamStatus));
        inOrder.verify(streamSessionLockManager).tryLockStream(advisoryConnection, streamId);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(componentEventProcessor).process(eventJsonEnvelope, component);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(streamRetryStatusManager).removeStreamRetryStatus(streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldProcessEventAndNotMarkStreamAsUpToDateWhenPositionNotEqualToLatestKnownPosition() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final long eventPositionInStream = 6L;

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
    }

    @Test
    public void shouldReturnEventNotFoundWhenNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final Connection advisoryConnection = mock(Connection.class);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(empty());
        when(nextEventSelector.selectNextEvent(source, component, empty())).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                streamSessionLockManager,
                transactionHandler,
                streamSelectorManager,
                nextEventSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(streamSessionLockManager).openLockConnection();
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelectorManager).selectStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, empty());
        inOrder.verify(transactionHandler).commit(userTransaction);

        verifyNoInteractions(componentEventProcessor);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(streamSessionLockManager, never()).unlockStream(any(), any());
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
    }

    @Test
    public void shouldRollbackWhenCommitFailsAfterNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final Connection advisoryConnection = mock(Connection.class);
        final RuntimeException commitException = new RuntimeException("Commit failed");

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(empty());
        when(nextEventSelector.selectNextEvent(source, component, empty())).thenReturn(empty());
        doThrow(commitException).when(transactionHandler).commit(userTransaction);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelectorManager,
                nextEventSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelectorManager).selectStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, empty());
        inOrder.verify(transactionHandler).commit(userTransaction);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRollbackWhenStreamSelectionFails() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final Connection advisoryConnection = mock(Connection.class);
        final RuntimeException selectionException = new RuntimeException("Selection failed");
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Failed to find stream to process, source: 'some-source', component: 'some-component'", selectionException);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getCause(), is(selectionException));
        assertThat(thrown.getMessage(), is("Failed to find stream to process, source: 'some-source', component: 'some-component'"));

        verify(transactionHandler).rollback(userTransaction);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
        verifyNoInteractions(nextEventSelector);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRollbackWhenEventNotFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final Connection advisoryConnection = mock(Connection.class);

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition));

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        verify(transactionHandler).rollback(userTransaction);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
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

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, of(streamErrorId));
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        doThrow(nullPointerException).when(componentEventProcessor).process(eventJsonEnvelope, component);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler,
                streamSessionLockManager);

        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, component, currentPosition, of(streamErrorId));

        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(transactionHandler, never()).commit(userTransaction);
    }

    @Test
    public void shouldReturnEventFoundAndRecordErrorIfNoPositionFoundInEvent() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(transactionHandler).rollback(userTransaction);
        verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler).onStreamProcessingFailure(eq(eventJsonEnvelope), any(MissingPositionInStreamException.class), eq(component), eq(currentPosition), eq(empty()));
        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
        verifyNoInteractions(componentEventProcessor);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenFindingEventFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final Connection advisoryConnection = mock(Connection.class);

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition));

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        verify(transactionHandler).rollback(userTransaction);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
    }

    @Test
    public void shouldReturnEventNotFoundWhenAdvisoryLockCannotBeAcquired() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(false);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        verifyNoInteractions(componentEventProcessor);
        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
    }

    @Test
    public void shouldMarkStreamAsFixedWhenStreamErrorIdPresentAndProcessingSucceeds() throws Exception {

        final UUID streamId = randomUUID();
        final UUID streamErrorId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 6L;
        final long eventPositionInStream = 6L;

        final Connection advisoryConnection = mock(Connection.class);
        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, of(streamErrorId));
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);

        when(streamSessionLockManager.openLockConnection()).thenReturn(advisoryConnection);
        when(streamSelectorManager.selectStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(streamSessionLockManager.tryLockStream(advisoryConnection, streamId)).thenReturn(true);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(streamErrorRepository).markStreamAsFixed(streamErrorId, streamId, source, component);
        verify(streamSessionLockManager).unlockStream(advisoryConnection, streamId);
        verify(streamSessionLockManager).closeQuietly(advisoryConnection);
        verify(transactionHandler, never()).rollback(userTransaction);
        verifyNoInteractions(streamErrorStatusHandler);
    }
}
