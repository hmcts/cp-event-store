package uk.gov.justice.services.event.sourcing.subscription.error;

import static java.util.UUID.fromString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorHandlingException;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.sql.Timestamp;
import java.util.Optional;
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
public class StreamErrorStatusHandlerTest {

    @Mock
    private ExceptionDetailsRetriever exceptionDetailsRetriever;

    @Mock
    private StreamErrorConverter streamErrorConverter;

    @Mock
    private StreamErrorRepository streamErrorRepository;

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private TransactionHandler transactionHandler;

    @Mock
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Mock
    private Logger logger;

    @Mock
    private StreamUpdateContext streamUpdateContext;

    @InjectMocks
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Test
    public void shouldCreateEventErrorFromExceptionAndJsonEnvelopeAndSave() throws Exception {

        final long currentStreamPosition = 123L;
        final NullPointerException nullPointerException = new NullPointerException();
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final String source = "SOME_SOURCE";
        final String component = "SOME_COMPONENT";

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component)).thenReturn(streamError);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, source, component, streamUpdateContext);

        final InOrder inOrder = inOrder(micrometerMetricsCounters, transactionHandler, streamErrorRepository);

        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError, currentStreamPosition);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
    }

    @Test
    public void shouldRollBackAndLogIfUpdatingErrorTableFails() throws Exception {

        final long currentStreamPosition = 123L;
        final NullPointerException nullPointerException = new NullPointerException();
        final StreamErrorHandlingException streamErrorHandlingException = new StreamErrorHandlingException("dsfkjh");
        final String source = "SOME_SOURCE";
        final String component = "SOME_COMPONENT";
        final UUID streamId = fromString("788cc64e-d31e-46fb-975f-b19042bb0a13");

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component)).thenReturn(streamError);
        when(streamError.streamErrorOccurrence()).thenReturn(streamErrorOccurrence);
        when(streamErrorOccurrence.streamId()).thenReturn(streamId);
        when(streamUpdateContext.currentStreamPosition()).thenReturn(currentStreamPosition);
        doThrow(streamErrorHandlingException).when(streamErrorRepository).markStreamAsErrored(streamError, currentStreamPosition);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, source, component, streamUpdateContext);

        final InOrder inOrder = inOrder(micrometerMetricsCounters, transactionHandler, streamErrorRepository, logger);

        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError, currentStreamPosition);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(logger).error("Failed to mark stream as errored: streamId '788cc64e-d31e-46fb-975f-b19042bb0a13'", streamErrorHandlingException);

        verify(transactionHandler, never()).commit(userTransaction);
    }

    @Test
    public void shouldMarkSameErrorHappenedWhenErrorIsSameAsBefore() throws Exception {

        final NullPointerException nullPointerException = new NullPointerException();
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError newStreamError = mock(StreamError.class);
        final StreamErrorOccurrence newStreamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final StreamErrorOccurrence existingStreamErrorOccurrence = mock(StreamErrorOccurrence.class);
        final String source = "SOME_SOURCE";
        final String component = "SOME_COMPONENT";
        final String errorHash = "same-error-hash";
        final Timestamp lastUpdatedAt = new Timestamp(System.currentTimeMillis());

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component)).thenReturn(newStreamError);
        when(newStreamError.streamErrorOccurrence()).thenReturn(newStreamErrorOccurrence);
        when(newStreamErrorOccurrence.hash()).thenReturn(errorHash);
        when(streamUpdateContext.existingStreamErrorDetails()).thenReturn(Optional.of(existingStreamErrorOccurrence));
        when(existingStreamErrorOccurrence.hash()).thenReturn(errorHash);
        when(streamUpdateContext.lastUpdatedAt()).thenReturn(lastUpdatedAt);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, source, component, streamUpdateContext);

        final InOrder inOrder = inOrder(micrometerMetricsCounters, transactionHandler, streamErrorRepository);

        inOrder.verify(micrometerMetricsCounters).incrementEventsFailedCount(source, component);
        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markSameErrorHappened(newStreamError, streamUpdateContext.currentStreamPosition(), lastUpdatedAt);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verify(streamErrorRepository, never()).markStreamAsErrored(newStreamError, streamUpdateContext.currentStreamPosition());
    }

    @Test
    public void shouldCreateEventErrorFromExceptionAndJsonEnvelopeAndSaveWhenNoStreamUpdateContext() throws Exception {

        final long currentPosition = 456L;
        final NullPointerException nullPointerException = new NullPointerException();
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final String component = "SOME_COMPONENT";

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component)).thenReturn(streamError);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, component, currentPosition);

        final InOrder inOrder = inOrder(transactionHandler, streamErrorRepository);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError, currentPosition);
        inOrder.verify(transactionHandler).commit(userTransaction);

        verify(transactionHandler, never()).rollback(userTransaction);
        verifyNoInteractions(micrometerMetricsCounters);
    }

    @Test
    public void shouldRollBackAndLogIfUpdatingErrorTableFailsWhenNoStreamUpdateContext() throws Exception {

        final long currentPosition = 789L;
        final NullPointerException nullPointerException = new NullPointerException();
        final StreamErrorHandlingException streamErrorHandlingException = new StreamErrorHandlingException("error occurred");
        final String component = "SOME_COMPONENT";
        final UUID streamId = fromString("a1b2c3d4-e5f6-7890-abcd-ef1234567890");

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final ExceptionDetails exceptionDetails = mock(ExceptionDetails.class);
        final StreamError streamError = mock(StreamError.class);
        final StreamErrorOccurrence streamErrorOccurrence = mock(StreamErrorOccurrence.class);

        when(exceptionDetailsRetriever.getExceptionDetailsFrom(nullPointerException)).thenReturn(exceptionDetails);
        when(streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component)).thenReturn(streamError);
        when(streamError.streamErrorOccurrence()).thenReturn(streamErrorOccurrence);
        when(streamErrorOccurrence.streamId()).thenReturn(streamId);
        doThrow(streamErrorHandlingException).when(streamErrorRepository).markStreamAsErrored(streamError, currentPosition);

        streamErrorStatusHandler.onStreamProcessingFailure(jsonEnvelope, nullPointerException, component, currentPosition);

        final InOrder inOrder = inOrder(transactionHandler, streamErrorRepository, logger);

        inOrder.verify(transactionHandler).begin(userTransaction);
        inOrder.verify(streamErrorRepository).markStreamAsErrored(streamError, currentPosition);
        inOrder.verify(transactionHandler).rollback(userTransaction);
        inOrder.verify(logger).error("Failed to mark stream as errored: streamId 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'", streamErrorHandlingException);

        verify(transactionHandler, never()).commit(userTransaction);
        verifyNoInteractions(micrometerMetricsCounters);
    }
}