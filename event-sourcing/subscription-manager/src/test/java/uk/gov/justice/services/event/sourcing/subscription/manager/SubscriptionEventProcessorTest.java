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
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

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
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @InjectMocks
    private SubscriptionEventProcessor subscriptionEventProcessor;

    @Test
    public void shouldSendEventToEventInterceptorChainAndMarkAsUpToDate() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(streamPositions.latestKnownStreamPosition()).thenReturn(eventPositionInStream);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(componentName)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, componentName);
        inOrder.verify(newStreamStatusRepository).setUpToDate(true, streamId, source, componentName);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldNotMarkAsUpToDateIfCurrentPositionIsNotEqualToLatestKnownPosition() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(streamPositions.latestKnownStreamPosition()).thenReturn(latestKnowPositionInStream);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(componentName)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, componentName);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, componentName);
    }

    @Test
    public void shouldSendEventFromEventBufferToEventInterceptorChainButNotMarkAsUpToDate() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(componentName)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName), is(true));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newStreamStatusRepository,
                newEventBufferRepository,
                streamErrorRepository,
                transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(newStreamStatusRepository).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(newEventBufferRepository).remove(streamId, source, componentName, eventPositionInStream);
        inOrder.verify(streamErrorRepository).markStreamAsFixed(streamId, source, componentName);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldDoNothingIfEventIsOutOfOrder() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_OUT_OF_ORDER);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName), is(false));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(interceptorChainProcessor, never()).process(interceptorContext);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, componentName, eventPositionInStream);
        verify(streamErrorRepository, never()).markStreamAsFixed(streamId, source, componentName);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, componentName);
        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldDoNothingIfEventIsAlreadyProcessed() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_ALREADY_PROCESSED);

        assertThat(subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName), is(false));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newEventBufferRepository,
                newStreamStatusRepository,
                streamErrorRepository,
                transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(interceptorChainProcessor, never()).process(interceptorContext);
        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, componentName, eventPositionInStream);
        verify(streamErrorRepository, never()).markStreamAsFixed(streamId, source, componentName);
        verify(newStreamStatusRepository, never()).setUpToDate(true, streamId, source, componentName);
        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfEventProcessingFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String componentName = "some-component-name";
        final String eventName = "some-event-name";
        final String source = "some-source";
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
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);
        when(interceptorChainProcessorProducer.produceLocalProcessor(componentName)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);
        doThrow(nullPointerException).when(interceptorChainProcessor).process(interceptorContext);

        final StreamProcessingException streamProcessingException = assertThrows(
                StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(streamProcessingException.getCause(), is(nullPointerException));
        assertThat(streamProcessingException.getMessage(), is("Failed to process event. name: 'some-event-name', eventId: 'ba0c36e1-659e-430c-9d33-67eda5ca70cd', streamId: '4f4815fa-825d-4869-a37f-e443dea21d18'"));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                interceptorChainProcessor,
                newStreamStatusRepository,
                newEventBufferRepository,
                transactionHandler,
                streamErrorStatusHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, componentName);

        verify(newStreamStatusRepository, never()).updateCurrentPosition(streamId, source, componentName, eventPositionInStream);
        verify(newEventBufferRepository, never()).remove(streamId, source, componentName, eventPositionInStream);
        verify(transactionHandler, never()).commit(userTransaction);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfLockRowAndGetPositionsFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String componentName = "some-component-name";
        final String eventName = "some-event-name";
        final String source = "some-source";
        final long eventPositionInStream = 876;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        doThrow(nullPointerException).when(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream);

        assertThrows(StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        final InOrder inOrder = inOrder(
                transactionHandler,
                streamErrorStatusHandler);

        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, componentName);

        verifyNoInteractions(eventProcessingStatusCalculator, interceptorChainProcessorProducer, newEventBufferRepository);
    }

    @Test
    public void shouldThrowStreamProcessingExceptionAndRecordErrorIfEventOrderingCalculationFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Ooops");

        final UUID eventId = fromString("ba0c36e1-659e-430c-9d33-67eda5ca70cd");
        final UUID streamId = fromString("4f4815fa-825d-4869-a37f-e443dea21d18");
        final String componentName = "some-component-name";
        final String eventName = "some-event-name";
        final String source = "some-source";
        final long eventPositionInStream = 876;

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(eventPositionInStream));
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                eventPositionInStream)).thenReturn(streamPositions);
        doThrow(nullPointerException).when(eventProcessingStatusCalculator).calculateEventOrderingStatus(streamPositions);

        assertThrows(StreamProcessingException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        final InOrder inOrder = inOrder(
                transactionHandler,
                streamErrorStatusHandler);

        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(streamErrorStatusHandler).onStreamProcessingFailure(eventJsonEnvelope, nullPointerException, componentName);

        verifyNoInteractions(interceptorChainProcessorProducer, newEventBufferRepository);
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoPositionFoundInEventEnvelope() throws Exception {

        final UUID eventId = fromString("b82b226a-3d8e-4fad-b456-5e747697f46d");
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(empty());

        final MissingPositionInStreamException missingPositionInStreamException = assertThrows(
                MissingPositionInStreamException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(missingPositionInStreamException.getMessage(), is("No position found in event: name 'some-event-name', eventId 'b82b226a-3d8e-4fad-b456-5e747697f46d'"));

        verifyNoInteractions(transactionHandler);
        verifyNoInteractions(newStreamStatusRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(streamErrorStatusHandler);
    }

    @Test
    public void shouldThrowMissingSourceExceptionIfNoSourceFoundInEventEnvelope() throws Exception {

        final UUID eventId = fromString("b82b226a-3d8e-4fad-b456-5e747697f46d");
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String componentName = "some-component-name";

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(empty());

        final MissingSourceException missingSourceException = assertThrows(
                MissingSourceException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(missingSourceException.getMessage(), is("No source found in event: name 'some-event-name', eventId 'b82b226a-3d8e-4fad-b456-5e747697f46d'"));

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
        final String componentName = "some-component-name";

        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(eventJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(empty());

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> subscriptionEventProcessor.processSingleEvent(eventJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event: name 'some-event-name', eventId 'b82b226a-3d8e-4fad-b456-5e747697f46d'"));

        verifyNoInteractions(transactionHandler);
        verifyNoInteractions(newStreamStatusRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(newEventBufferRepository);
        verifyNoInteractions(streamErrorStatusHandler);
    }
}