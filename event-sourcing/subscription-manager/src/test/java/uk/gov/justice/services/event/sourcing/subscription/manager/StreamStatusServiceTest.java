package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_ALREADY_PROCESSED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_CORRECTLY_ORDERED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventOrderingStatus.EVENT_OUT_OF_ORDER;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusException;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class StreamStatusServiceTest {


    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;

    @Mock
    private NewEventBufferManager newEventBufferManager;

    @Mock
    private LatestKnownPositionAndIsUpToDateUpdater latestKnownPositionAndIsUpToDateUpdater;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private UtcClock clock;

    @Mock
    private Logger logger;

    @InjectMocks
    private StreamStatusService streamStatusService;

    @Test
    public void shouldCreateStreamAndUpdatePositionsIfNecessaryInSingleTransaction() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final long incomingPositionInStream = 87687;
        final ZonedDateTime updatedAt = new UtcClock().now();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(incomingPositionInStream));
        when(clock.now()).thenReturn(updatedAt);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_CORRECTLY_ORDERED);

        assertThat(streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName), is(EVENT_CORRECTLY_ORDERED));

        final InOrder inOrder = inOrder(transactionHandler, newStreamStatusRepository, latestKnownPositionAndIsUpToDateUpdater);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                false);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream);
        inOrder.verify(latestKnownPositionAndIsUpToDateUpdater).updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(newEventBufferManager, never()).addToBuffer(incomingJsonEnvelope, componentName);
    }

    @Test
    public void shouldBufferEventIfEventOutOfOrder() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final String source = "some-source";
        final long incomingPositionInStream = 87687;
        final ZonedDateTime updatedAt = new UtcClock().now();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(incomingPositionInStream));
        when(clock.now()).thenReturn(updatedAt);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream)).thenReturn(streamPositions);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_OUT_OF_ORDER);

        assertThat(streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName), is(EVENT_OUT_OF_ORDER));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                latestKnownPositionAndIsUpToDateUpdater,
                newEventBufferManager);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                false);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream);
        inOrder.verify(latestKnownPositionAndIsUpToDateUpdater).updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName);
        inOrder.verify(newEventBufferManager).addToBuffer(incomingJsonEnvelope, componentName);
        inOrder.verify(transactionHandler).commit(userTransaction);
    }

    @Test
    public void shouldLogAndNothingElseIfEventPreviouslyProcessed() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("0fac71b5-3e61-4ec1-b1d5-e8d85b1e0100");
        final UUID streamId = fromString("1bc83024-d11a-4177-8892-1592b3741cc0");
        final String source = "some-source";
        final long incomingPositionInStream = 23;
        final long currentStreamPosition = 99;
        final ZonedDateTime updatedAt = new UtcClock().now();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);
        final StreamPositions streamPositions = mock(StreamPositions.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(incomingPositionInStream));
        when(clock.now()).thenReturn(updatedAt);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream)).thenReturn(streamPositions);
        when(streamPositions.currentStreamPosition()).thenReturn(currentStreamPosition);
        when(streamPositions.incomingEventPosition()).thenReturn(incomingPositionInStream);
        when(eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions)).thenReturn(EVENT_ALREADY_PROCESSED);

        assertThat(streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName), is(EVENT_ALREADY_PROCESSED));

        final InOrder inOrder = inOrder(
                transactionHandler,
                newStreamStatusRepository,
                latestKnownPositionAndIsUpToDateUpdater,
                logger,
                newEventBufferManager);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                false);
        inOrder.verify(newStreamStatusRepository).lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream);
        inOrder.verify(latestKnownPositionAndIsUpToDateUpdater).updateIfNecessary(
                streamPositions,
                streamId,
                source,
                componentName);
        inOrder.verify(logger).info("Duplicate incoming event detected. Event already processed; ignoring. eventId: '0fac71b5-3e61-4ec1-b1d5-e8d85b1e0100', streamId: '1bc83024-d11a-4177-8892-1592b3741cc0', incomingEventPositionInStream '23' currentStreamPosition: '99'");
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(newEventBufferManager, never()).addToBuffer(incomingJsonEnvelope, componentName);
    }

    @Test
    public void shouldThrowStreamStatusExceptionIfUpdatingStreamStatusTableFails() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException("Oh man");
        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("092c23a0-40b3-4137-8016-501e6840e95c");
        final UUID streamId = fromString("bfb89fb7-79af-4679-b01d-cd46f5b0b38f");
        final String source = "some-source";
        final long incomingPositionInStream = 87687;
        final ZonedDateTime updatedAt = new UtcClock().now();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(incomingPositionInStream));
        when(clock.now()).thenReturn(updatedAt);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream)).thenThrow(nullPointerException);

        final StreamStatusException streamStatusException = assertThrows(
                StreamStatusException.class,
                () -> streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName));

        assertThat(streamStatusException.getCause(), is(nullPointerException));
        assertThat(streamStatusException.getMessage(), is("Failed to update stream_status/event_buffer for eventId '092c23a0-40b3-4137-8016-501e6840e95c', eventName 'some-name', streamId 'bfb89fb7-79af-4679-b01d-cd46f5b0b38f'"));

        final InOrder inOrder = inOrder(transactionHandler);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(transactionHandler).rollback(userTransaction);

        verify(transactionHandler, never()).commit(userTransaction);
    }

    @Test
    public void shouldRollBackAndThrowStreamStatusExceptionIfCalculatingCurrentStatusFails() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("d4666897-7420-4e18-8796-209e8f085695");
        final UUID streamId = fromString("85831bb3-1257-4096-8844-ee7d88e86a1f");
        final String source = "some-source";
        final long incomingPositionInStream = 87687;
        final ZonedDateTime updatedAt = new UtcClock().now();

        final NullPointerException nullPointerException = new NullPointerException("Bunnies");

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(of(incomingPositionInStream));
        when(clock.now()).thenReturn(updatedAt);
        when(newStreamStatusRepository.lockRowAndGetPositions(
                streamId,
                source,
                componentName,
                incomingPositionInStream)).thenThrow(nullPointerException);

        final StreamStatusException streamStatusException = assertThrows(
                StreamStatusException.class,
                () -> streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName));

        assertThat(streamStatusException.getCause(), is(nullPointerException));
        assertThat(streamStatusException.getMessage(), is("Failed to update stream_status/event_buffer for eventId 'd4666897-7420-4e18-8796-209e8f085695', eventName 'some-name', streamId '85831bb3-1257-4096-8844-ee7d88e86a1f'"));

        final InOrder inOrder = inOrder(transactionHandler, newStreamStatusRepository, latestKnownPositionAndIsUpToDateUpdater);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(transactionHandler).rollback(userTransaction);
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdFoundInIncomingEvent() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("ab7be3df-8fbe-41e7-a511-e1d6af297491");

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(empty());

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event: name 'some-name', eventId 'ab7be3df-8fbe-41e7-a511-e1d6af297491'"));
    }

    @Test
    public void shouldThrowMissingSourceExceptionIfNoSourceFoundInIncomingEvent() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("abffce7f-6ccb-413b-a43e-9bb61386c000");
        final UUID streamId = randomUUID();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(empty());

        final MissingSourceException missingSourceException = assertThrows(
                MissingSourceException.class,
                () -> streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName));

        assertThat(missingSourceException.getMessage(), is("No source found in event: name 'some-name', eventId 'abffce7f-6ccb-413b-a43e-9bb61386c000'"));
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoStreamIdFoundInIncomingEvent() throws Exception {

        final String name = "some-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("7695b98f-99a8-49b0-b00e-2d9b2c1c4d9a");
        final UUID streamId = randomUUID();
        final String source = "some-source";

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(incomingJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(name);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(metadata.position()).thenReturn(empty());

        final MissingPositionInStreamException missingStreamIdException = assertThrows(
                MissingPositionInStreamException.class,
                () -> streamStatusService.handleStreamStatusUpdates(incomingJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No position found in event: name 'some-name', eventId '7695b98f-99a8-49b0-b00e-2d9b2c1c4d9a'"));
    }
}