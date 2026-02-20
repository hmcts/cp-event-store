package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.UUID;
import javax.transaction.UserTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamRetryStatusManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;
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

@ExtendWith(MockitoExtension.class)
public class StreamEventProcessorTest {

    @Mock
    private InterceptorContextProvider interceptorContextProvider;

    @Mock
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Mock
    public InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Mock
    private StreamSelector streamSelector;

    @Mock
    private TransactionalEventReader transactionalEventReader;

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
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler,
                streamRetryStatusManager);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(streamRetryStatusManager).removeStreamRetryStatus(streamId, source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);
        inOrder.verify(streamEventLoggerMetadataAdder).clearMdc();

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
        verifyNoInteractions(streamErrorRepository);
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
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);
        inOrder.verify(streamEventLoggerMetadataAdder).clearMdc();

        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
        verifyNoInteractions(streamErrorRepository);
    }

    @Test
    public void shouldReturnEventNotFoundWhenNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionalEventReader, never()).readNextEvent(any(), any(), any());
        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldRollbackWhenCommitFailsAfterNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final RuntimeException commitException = new RuntimeException("Commit failed");

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(empty());
        doThrow(commitException).when(transactionHandler).commit(userTransaction);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_NOT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(transactionalEventReader, never()).readNextEvent(any(), any(), any());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenStreamSelectionFails() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final RuntimeException selectionException = new RuntimeException("Selection failed");

        when(streamSelector.findStreamToProcess(source, component)).thenThrow(selectionException);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getCause(), is(selectionException));
        assertThat(streamProcessingException.getMessage(), is("Failed to find stream to process, source: 'some-source', component: 'some-component'"));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(transactionalEventReader, never()).readNextEvent(any(), any(), any());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenEventNotFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(empty());

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getMessage(), is("Unable to find next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
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
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, component, currentPosition, of(streamErrorId));
        inOrder.verify(streamEventLoggerMetadataAdder).clearMdc();

        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).commit(userTransaction);
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

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                streamEventLoggerMetadataAdder,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eq(eventJsonEnvelope), any(MissingPositionInStreamException.class), eq(component), eq(currentPosition), eq(empty()));
        inOrder.verify(streamEventLoggerMetadataAdder).clearMdc();

        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verifyNoInteractions(streamEventValidator);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenFindingEventFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final RuntimeException eventFindingException = new RuntimeException("Event finding failed");

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenThrow(eventFindingException);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getMessage(), is("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenEventReaderFails() {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final RuntimeException readerException = new RuntimeException("Reader failed");

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition, empty());

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenThrow(readerException);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getMessage(), is("Failed to pull next event to process for streamId: '%s', position: %d, latestKnownPosition: %d".formatted(streamId, currentPosition, latestKnownPosition)));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
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
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(transactionalEventReader.readNextEvent(source, streamId, currentPosition)).thenReturn(of(eventJsonEnvelope));
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(EVENT_FOUND));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionalEventReader,
                streamEventLoggerMetadataAdder,
                streamEventValidator,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler,
                streamRetryStatusManager,
                streamErrorRepository);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionalEventReader).readNextEvent(source, streamId, currentPosition);
        inOrder.verify(streamEventLoggerMetadataAdder).addRequestDataToMdc(eventJsonEnvelope, component);
        inOrder.verify(streamEventValidator).validate(eventJsonEnvelope, source, component);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(streamRetryStatusManager).removeStreamRetryStatus(streamId, source, component);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamErrorId, streamId, source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);
        inOrder.verify(streamEventLoggerMetadataAdder).clearMdc();

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verifyNoInteractions(streamErrorStatusHandler);
    }
}
