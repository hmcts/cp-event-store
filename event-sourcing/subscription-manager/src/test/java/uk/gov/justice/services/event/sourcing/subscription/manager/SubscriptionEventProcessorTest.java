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
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

@ExtendWith(MockitoExtension.class)
public class SubscriptionEventProcessorTest {

    @Mock
    private InterceptorContextProvider interceptorContextProvider;

    @Mock
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Mock
    public InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private NewEventBufferRepository newEventBufferRepository;

    @Mock
    private StreamErrorRepository streamErrorRepository;

    @Mock
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @InjectMocks
    private SubscriptionEventProcessor subscriptionEventProcessor;

    @Test
    public void shouldSendEventToEventInterceptorChainAndMarkAsUpToDate() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long eventPositionInStream = 7686;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(streamPositions.latestKnownStreamPosition()).thenReturn(eventPositionInStream);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, component, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, component);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldNotMarkAsUpToDateIfCurrentPositionIsNotEqualToLatestKnownPosition() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long eventPositionInStream = 7686;
        final long latestKnowPositionInStream = 1000000;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(streamPositions.latestKnownStreamPosition()).thenReturn(latestKnowPositionInStream);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler,
                micrometerMetricsCounters);

        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, component, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldSendEventFromEventBufferToEventInterceptorChainButNotMarkAsUpToDate() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component-name";
        final long eventPositionInStream = 7686;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newStreamStatusRepository,
                newEventBufferRepository,
                streamErrorRepository,
                transactionHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, component, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, component);
        inOrder.verify(micrometerMetricsCounters).incrementEventsSucceededCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
    }

    @Test
    public void shouldDoNothingIfEventIsOutOfOrder() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long eventPositionInStream = 7686;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_OUT_OF_ORDER);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component), is(false));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler,
                micrometerMetricsCounters);

        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(interceptorChainProcessor, never()).process(interceptorContext);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, component, eventPositionInStream);
        verify(streamErrorRepository, never()).markStreamAsFixed(streamId, source, component);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
    }

    @Test
    public void shouldDoNothingIfEventIsAlreadyProcessed() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";
        final long eventPositionInStream = 7686;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_ALREADY_PROCESSED);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component), is(false));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(micrometerMetricsCounters).incrementEventsIgnoredCount(source, component);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(interceptorChainProcessor, never()).process(interceptorContext);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, component, eventPositionInStream);
        verify(streamErrorRepository, never()).markStreamAsFixed(streamId, source, component);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, component);
        verify(transactionHandler, never()).rollback(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsFailedCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfEventProcessingFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String source = "some-source";
        final String component = "some-component";
        final String eventName = "some-event-name";
        final long eventPositionInStream = 876;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component));

        assertThat(streamProcessingException.getCause(), is(nullPointerException));
        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'some-event-name', eventId: 'ba0c36e1-659e-430c-9d33-67eda5ca70cd', streamId: '4f4815fa-825d-4869-a37f-e443dea21d18'"));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newStreamStatusRepository,
                newEventBufferRepository,
                transactionHandler,
                streamErrorStatusHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, source, component);

        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, component, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, component, eventPositionInStream);
        verify(transactionHandler, never()).commit(userTransaction);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsIgnoredCount(source, component);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfLockRowAndGetPositionsFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String source = "some-source";
        final String component = "some-component";
        final String eventName = "some-event-name";
        final long eventPositionInStream = 876;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        doThrow(nullPointerException).when(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream);

        assertThrows(StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component));

        final InOrder inOrder = inOrder(
                transactionHandler,
                streamErrorStatusHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, source, component);

        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsIgnoredCount(source, component);
        verifyNoInteractions(eventProcessingStatusCalculator, interceptorChainProcessorProducer, newEventBufferRepository);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfEventOrderingCalculationFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String source = "some-source";
        final String component = "some-component";
        final String eventName = "some-event-name";
        final long eventPositionInStream = 876;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                component,
                eventPositionInStream)).thenReturn(streamPositions);
        doThrow(nullPointerException).when(eventProcessingStatusCalculator).calculateEventOrderingStatus(streamPositions);

        assertThrows(StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component));

        final InOrder inOrder = inOrder(
                transactionHandler,
                streamErrorStatusHandler,
                micrometerMetricsCounters);

        inOrder.verify(micrometerMetricsCounters).incrementEventsProcessedCount(source, component);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, source, component);

        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsIgnoredCount(source, component);
        verifyNoInteractions(interceptorChainProcessorProducer, newEventBufferRepository);
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoPositionFoundInEventEnvelope() throws Exception {

        final UUID eventId = fromString("b82b226a-3d8e-4fad-b456-5e747697f46d");
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(eventJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(empty());

        final MissingPositionInStreamException missingPositionInStreamException = assertThrows(
                MissingPositionInStreamException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component));

        assertThat(missingPositionInStreamException.getMessage(), is("No position found in event: name 'some-event-name', eventId 'b82b226a-3d8e-4fad-b456-5e747697f46d'"));

        verify(micrometerMetricsCounters, never()).incrementEventsProcessedCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsIgnoredCount(source, component);
        verifyNoInteractions(transactionHandler);
        verifyNoInteractions(newStreamStatusRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdFoundInEventEnvelope() throws Exception {

        final UUID eventId = fromString("b82b226a-3d8e-4fad-b456-5e747697f46d");
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String component = "some-component";

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(empty());

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, component));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event: name 'some-event-name', eventId 'b82b226a-3d8e-4fad-b456-5e747697f46d'"));

        verifyNoInteractions(transactionHandler);
        verifyNoInteractions(newStreamStatusRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(streamErrorStatusHandler);
        verify(micrometerMetricsCounters, never()).incrementEventsProcessedCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsSucceededCount(source, component);
        verify(micrometerMetricsCounters, never()).incrementEventsIgnoredCount(source, component);
    }
}
