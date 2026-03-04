package uk.gov.justice.services.event.sourcing.subscription.error;

import java.sql.Connection;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.UserTransaction;
import org.slf4j.Logger;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamSessionLockManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

public class StreamErrorStatusHandler {

    @Inject
    private ExceptionDetailsRetriever exceptionDetailsRetriever;

    @Inject
    private StreamErrorConverter streamErrorConverter;

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Inject
    private StreamRetryStatusManager streamRetryStatusManager;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Inject
    private StreamErrorPersistence streamErrorPersistence;

    @Inject
    private StreamSessionLockManager streamSessionLockManager;

    @Inject
    private Logger logger;

    public void onStreamProcessingFailure(final JsonEnvelope jsonEnvelope, final Throwable exception, final String source, final String component, final StreamUpdateContext streamUpdateContext) {

        micrometerMetricsCounters.incrementEventsFailedCount(source, component);

        final ExceptionDetails exceptionDetails = exceptionDetailsRetriever.getExceptionDetailsFrom(exception);
        final StreamError newStreamError = streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component);
        final UUID streamId = newStreamError.streamErrorOccurrence().streamId();
        try {
            transactionHandler.begin(userTransaction);

            if (isErrorSameAsBefore(newStreamError, streamUpdateContext))
                streamErrorRepository.markSameErrorHappened(streamUpdateContext.streamErrorId().get(), streamId, source, component);
            else {
                streamErrorRepository.markStreamAsErrored(newStreamError, streamUpdateContext.currentStreamPosition());
            }

            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            logger.error("Failed to mark stream as errored: streamId '%s'".formatted(streamId), e);
        }
    }

    public void onStreamProcessingFailure(final JsonEnvelope jsonEnvelope, final Throwable exception, final String component, final long currentPosition, final Optional<UUID> existingErrorId) {
        final ExceptionDetails exceptionDetails = exceptionDetailsRetriever.getExceptionDetailsFrom(exception);
        final StreamError newStreamError = streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component);
        final UUID streamId = newStreamError.streamErrorOccurrence().streamId();
        final String source = newStreamError.streamErrorOccurrence().source();
        try {
            transactionHandler.begin(userTransaction);

            if (existingErrorId.isPresent() && isErrorSameAsBefore(newStreamError, existingErrorId.get())) {
                streamErrorRepository.markSameErrorHappened(existingErrorId.get(), streamId, source, component);
            } else {
                streamErrorRepository.markStreamAsErrored(newStreamError, currentPosition);
            }

            streamRetryStatusManager.updateStreamRetryCountAndNextRetryTime(streamId, source, component);

            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            logger.error("Failed to mark stream as errored: streamId '%s'".formatted(streamId), e);
        }
    }

    /**
     * Records a stream processing error directly on the provided physical connection,
     * bypassing JTA. Used by the pull-based path (StreamEventProcessor) where an advisory
     * lock already guarantees exclusive access to the stream, making row-level locking
     * (lockStreamForUpdate) unnecessary.
     */
    public void recordStreamProcessingError(
            final Connection connection,
            final JsonEnvelope jsonEnvelope,
            final Throwable exception,
            final String component,
            final Optional<UUID> existingErrorId) {

        final ExceptionDetails exceptionDetails = exceptionDetailsRetriever.getExceptionDetailsFrom(exception);
        final StreamError newStreamError = streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component);
        final StreamErrorOccurrence occurrence = newStreamError.streamErrorOccurrence();
        final UUID streamId = occurrence.streamId();
        final String source = occurrence.source();

        try {
            streamSessionLockManager.beginDirectTransaction(connection);

            if (existingErrorId.isPresent() && hasMatchingErrorHash(newStreamError, existingErrorId.get(), connection)) {
                streamStatusErrorPersistence.updateStreamErrorOccurredAt(existingErrorId.get(), streamId, source, component, connection);
            } else {
                if (streamErrorPersistence.save(newStreamError, connection)) {
                    streamStatusErrorPersistence.markStreamAsErrored(
                            streamId,
                            occurrence.id(),
                            occurrence.positionInStream(),
                            component,
                            source,
                            connection);
                }
            }

            streamRetryStatusManager.updateStreamRetryCountAndNextRetryTime(streamId, source, component, connection);

            streamSessionLockManager.commitDirectTransaction(connection);
        } catch (final Exception e) {
            streamSessionLockManager.rollbackDirectTransaction(connection);
            logger.error("Failed to record stream processing error: streamId '%s'".formatted(streamId), e);
        } finally {
            streamSessionLockManager.restoreAutoCommit(connection);
        }
    }

    public void onStreamProcessingSuccess(final UUID streamId, final String source, final String component, final Optional<UUID> existingErrorId) {
        streamRetryStatusManager.removeStreamRetryStatus(streamId, source, component);
        existingErrorId.ifPresent(errorId -> streamErrorRepository.markStreamAsFixed(errorId, streamId, source, component));
    }

    private boolean hasMatchingErrorHash(final StreamError newStreamError, final UUID existingErrorId, final Connection connection) {
        final Optional<String> existingHash = streamErrorPersistence.findByErrorId(existingErrorId, connection)
                .map(StreamError::streamErrorOccurrence)
                .map(StreamErrorOccurrence::hash);
        return hashMatches(newStreamError, existingHash);
    }

    private boolean isErrorSameAsBefore(final StreamError newStreamError, final StreamUpdateContext streamUpdateContext) {
        final Optional<String> existingHash = Optional.of(streamUpdateContext)
                .flatMap(StreamUpdateContext::existingStreamErrorDetails)
                .map(StreamErrorOccurrence::hash);
        return hashMatches(newStreamError, existingHash);
    }

    private boolean isErrorSameAsBefore(final StreamError newStreamError, final UUID existingErrorId) {
        final Optional<String> existingHash = streamErrorRepository.findByErrorId(existingErrorId)
                .map(StreamError::streamErrorOccurrence)
                .map(StreamErrorOccurrence::hash);
        return hashMatches(newStreamError, existingHash);
    }

    private boolean hashMatches(final StreamError newStreamError, final Optional<String> existingHash) {
        return existingHash
                .map(hash -> Objects.equals(newStreamError.streamErrorOccurrence().hash(), hash))
                .orElse(false);
    }
}
