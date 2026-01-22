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
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Mock
    private EventConverter eventConverter;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @InjectMocks
    private StreamEventProcessor streamEventProcessor;

    @Test
    public void shouldProcessEventAndMarkStreamAsUpToDate() throws Exception {

        final UUID streamId = randomUUID();
        final UUID eventId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 6L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition);
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, currentPosition)).thenReturn(of(linkedEvent));
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(eventJsonEnvelope);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(true));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                linkedEventSourceProvider,
                linkedEventSource,
                eventConverter,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(linkedEventSourceProvider).getLinkedEventSource(source);
        inOrder.verify(linkedEventSource).findNextEventInTheStreamAfterPosition(streamId, currentPosition);
        inOrder.verify(eventConverter).envelopeOf(linkedEvent);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler, never()).onStreamProcessingFailure(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldProcessEventAndNotMarkStreamAsUpToDateWhenPositionNotEqualToLatestKnownPosition() throws Exception {

        final UUID streamId = randomUUID();
        final UUID eventId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition);
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, currentPosition)).thenReturn(of(linkedEvent));
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(eventJsonEnvelope);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(true));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                linkedEventSourceProvider,
                linkedEventSource,
                eventConverter,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                newStreamStatusRepository,
                micrometerMetricsCounters,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(linkedEventSourceProvider).getLinkedEventSource(source);
        inOrder.verify(linkedEventSource).findNextEventInTheStreamAfterPosition(streamId, currentPosition);
        inOrder.verify(eventConverter).envelopeOf(linkedEvent);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler, never()).onStreamProcessingFailure(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldReturnFalseWhenNoStreamFound() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(empty());

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(false));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                transactionHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(linkedEventSourceProvider, never()).getLinkedEventSource(any());
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

        assertThat(streamEventProcessor.processSingleEvent(source, component), is(false));

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

        verify(linkedEventSourceProvider, never()).getLinkedEventSource(any());
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

        verify(linkedEventSourceProvider, never()).getLinkedEventSource(any());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(streamErrorStatusHandler, never()).onStreamProcessingFailure(any(), any(), any(), any(), any());
    }

    @Test
    public void shouldThrowStreamProcessingExceptionWhenEventNotFound() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition);
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, currentPosition)).thenReturn(empty());

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'null', eventId: 'null', streamId: '%s'".formatted(streamId)));
        assertThat(streamProcessingException.getCause().getMessage(), is("Failed to find event to process, streamId: '" + streamId + "', position: '5'"));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                linkedEventSourceProvider,
                linkedEventSource,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(linkedEventSourceProvider).getLinkedEventSource(source);
        inOrder.verify(linkedEventSource).findNextEventInTheStreamAfterPosition(streamId, currentPosition);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eq(null), any(), eq(component), eq(currentPosition));

        verify(eventConverter, never()).envelopeOf(any());
        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfEventProcessingFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID streamId = randomUUID();
        final UUID eventId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;
        final long eventPositionInStream = 6L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition);
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, currentPosition)).thenReturn(of(linkedEvent));
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(eventJsonEnvelope);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getCause(), is(nullPointerException));
        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'some-event-name', eventId: '" + eventId + "', streamId: '" + streamId + "'"));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                linkedEventSourceProvider,
                linkedEventSource,
                eventConverter,
                interceptorChainProcessorProducer,
                interceptorContextProvider,
                interceptorChainProcessor,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(linkedEventSourceProvider).getLinkedEventSource(source);
        inOrder.verify(linkedEventSource).findNextEventInTheStreamAfterPosition(streamId, currentPosition);
        inOrder.verify(eventConverter).envelopeOf(linkedEvent);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, component, currentPosition);

        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).commit(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoPositionFoundInEvent() throws Exception {

        final UUID streamId = randomUUID();
        final UUID eventId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long currentPosition = 5L;
        final long latestKnownPosition = 10L;

        final LockedStreamStatus lockedStreamStatus = new LockedStreamStatus(streamId, currentPosition, latestKnownPosition);
        final LinkedEventSource linkedEventSource = mock(LinkedEventSource.class);
        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(streamSelector.findStreamToProcess(source, component)).thenReturn(of(lockedStreamStatus));
        when(linkedEventSourceProvider.getLinkedEventSource(source)).thenReturn(linkedEventSource);
        when(linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, currentPosition)).thenReturn(of(linkedEvent));
        when(eventConverter.envelopeOf(linkedEvent)).thenReturn(eventJsonEnvelope);
        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.position()).thenReturn(empty());

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> streamEventProcessor.processSingleEvent(source, component));

        assertThat(streamProcessingException.getCause().getMessage(), is("No position found in event: name 'some-event-name', eventId '" + eventId + "'"));

        final InOrder inOrder = inOrder(
                micrometerMetricsCounters,
                transactionHandler,
                streamSelector,
                linkedEventSourceProvider,
                linkedEventSource,
                eventConverter,
                transactionHandler,
                micrometerMetricsCounters,
                streamErrorStatusHandler);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamSelector).findStreamToProcess(source, component);
        inOrder.verify(linkedEventSourceProvider).getLinkedEventSource(source);
        inOrder.verify(linkedEventSource).findNextEventInTheStreamAfterPosition(streamId, currentPosition);
        inOrder.verify(eventConverter).envelopeOf(linkedEvent);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eq(eventJsonEnvelope), any(MissingPositionInStreamException.class), eq(component), eq(currentPosition));

        verify(interceptorChainProcessorProducer, never()).produceLocalProcessor(any());
        verify(newStreamStatusRepository, never()).updateCurrentPosition(any(), any(), any(), anyLong());
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
    }
}
