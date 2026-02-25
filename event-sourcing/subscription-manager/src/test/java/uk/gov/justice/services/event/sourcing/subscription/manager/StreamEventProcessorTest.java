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
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler.SavepointContext;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.sql.SQLException;
import java.util.UUID;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamEventProcessorTest {

    @Mock
    private ComponentEventProcessor componentEventProcessor;

    @Mock
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Mock
    private OldestStreamSelector oldestStreamSelector;

    @Mock
    private NextEventSelector nextEventSelector;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Mock
    private StreamEventValidator streamEventValidator;

    @Mock
    private Logger logger;

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
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                oldestStreamSelector,
                nextEventSelector,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                componentEventProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin();
        inOrder.verify(oldestStreamSelector).findStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, of(lockedStreamStatus));
        inOrder.verify(transactionHandler).createSavepointContext();
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(componentEventProcessor).process(eventJsonEnvelope, component);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(transactionHandler).releaseSavepointAndCommit(savepointContext);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);

        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verify(transactionHandler, never()).rollback();
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

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler).releaseSavepointAndCommit(savepointContext);
        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verify(transactionHandler, never()).rollback();
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldReturnEventNotFoundWhenNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(empty());
        when(nextEventSelector.selectNextEvent(source, component, empty())).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                oldestStreamSelector,
                nextEventSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin();
        inOrder.verify(oldestStreamSelector).findStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, empty());
        inOrder.verify(transactionHandler).commitWithFallbackToRollback();

        verifyNoInteractions(componentEventProcessor);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldRollbackAndRethrowWhenStreamSelectionFails() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final RuntimeException selectionException = new RuntimeException("Selection failed");
        final StreamProcessingException streamProcessingException = new StreamProcessingException(
                "Failed to find stream to process, source: 'some-source', component: 'some-component'", selectionException);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class, () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getCause(), is(selectionException));
        assertThat(thrown.getMessage(), is("Failed to find stream to process, source: 'some-source', component: 'some-component'"));

        verify(transactionHandler).rollback();
        verifyNoInteractions(nextEventSelector);
    }

    @Test
    public void shouldRollbackAndRethrowWhenEventNotFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final String expectedMessage = "Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d"
                .formatted(streamId, currentPosition, latestKnownPosition);
        final StreamProcessingException streamProcessingException = new StreamProcessingException(expectedMessage);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class, () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is(expectedMessage));

        verify(transactionHandler).rollback();
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
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        doThrow(nullPointerException).when(componentEventProcessor).process(eventJsonEnvelope, component);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).rollbackSavepointAndRestartIfTainted(savepointContext);
        inOrder.verify(streamErrorStatusHandler).recordStreamError(eventJsonEnvelope, nullPointerException, component, currentPosition, of(streamErrorId));
        inOrder.verify(transactionHandler).commit();

        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
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
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(transactionHandler).rollbackSavepointAndRestartIfTainted(savepointContext);
        verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler).recordStreamError(
                eq(eventJsonEnvelope), any(MissingPositionInStreamException.class), eq(component), eq(currentPosition), eq(empty()));
        verify(transactionHandler).commit();
        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verifyNoInteractions(componentEventProcessor);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenFindingEventFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final String expectedMessage = "Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d"
                .formatted(streamId, currentPosition, latestKnownPosition);
        final StreamProcessingException streamProcessingException = new StreamProcessingException(expectedMessage);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class, () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is(expectedMessage));

        verify(transactionHandler).rollback();
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenEventConverterFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final String expectedMessage = "Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d"
                .formatted(streamId, currentPosition, latestKnownPosition);
        final StreamProcessingException streamProcessingException = new StreamProcessingException(expectedMessage);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenThrow(streamProcessingException);

        final StreamProcessingException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                StreamProcessingException.class, () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(thrown.getMessage(), is(expectedMessage));

        verify(transactionHandler).rollback();
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

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, of(streamErrorId));
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                oldestStreamSelector,
                nextEventSelector,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                componentEventProcessor,
                newStreamStatusRepository,
                streamErrorStatusHandler,
                micrometerMetricsCounters,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin();
        inOrder.verify(oldestStreamSelector).findStreamToProcess(source, component);
        inOrder.verify(nextEventSelector).selectNextEvent(source, component, of(lockedStreamStatus));
        inOrder.verify(transactionHandler).createSavepointContext();
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(componentEventProcessor).process(eventJsonEnvelope, component);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(streamErrorStatusHandler).markStreamAsFixed(streamErrorId, streamId, source, component);
        inOrder.verify(transactionHandler).releaseSavepointAndCommit(savepointContext);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);

        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verify(transactionHandler, never()).rollback();
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldRollbackEntireTransactionWhenRecordStreamErrorFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");
        final RuntimeException errorRecordingException = new RuntimeException("Error recording failed");

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
        final SavepointContext savepointContext = mock(SavepointContext.class);

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenReturn(savepointContext);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        doThrow(nullPointerException).when(componentEventProcessor).process(eventJsonEnvelope, component);
        doThrow(errorRecordingException).when(streamErrorStatusHandler).recordStreamError(
                eq(eventJsonEnvelope), eq(nullPointerException), eq(component), eq(currentPosition), eq(empty()));

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler,
                logger,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).rollbackSavepointAndRestartIfTainted(savepointContext);
        inOrder.verify(streamErrorStatusHandler).recordStreamError(
                eq(eventJsonEnvelope), eq(nullPointerException), eq(component), eq(currentPosition), eq(empty()));
        inOrder.verify(logger).error(eq("Failed to record stream error for streamId '{}': {}"),
                eq(streamId), eq(errorRecordingException.getMessage()), eq(errorRecordingException));
        inOrder.verify(transactionHandler).rollback();

        verify(streamEventLoggerMetadataAdder).clearMdc();
        verify(transactionHandler).closeSavepointContext(savepointContext);
        verify(transactionHandler, never()).commit();
    }

    @Test
    public void shouldRollbackTransactionWhenSavepointContextCreationFails() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final PulledEvent pulledEvent = new PulledEvent(eventJsonEnvelope, lockedStreamStatus);
        final SQLException sqlException = new SQLException("Savepoint failed");

        when(oldestStreamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(nextEventSelector.selectNextEvent(source, component, of(lockedStreamStatus))).thenReturn(of(pulledEvent));
        when(transactionHandler.createSavepointContext()).thenThrow(sqlException);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        verify(logger).error(eq("Failed to create savepoint context for streamId '{}': {}"), eq(streamId), eq("Savepoint failed"), eq(sqlException));
        verify(transactionHandler).rollback();
        verifyNoInteractions(componentEventProcessor);
    }
}
