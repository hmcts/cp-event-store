package uk.gov.justice.services.event.sourcing.subscription.error;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.UserTransaction;
import org.slf4j.Logger;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorOccurrence;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;
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

            streamRetryStatusManager.updateStreamRetryCountAndNextRetryTime(streamId, source, component);

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

    private boolean isErrorSameAsBefore(final StreamError newStreamError, final StreamUpdateContext streamUpdateContext) {
        return Optional.of(streamUpdateContext)
                .flatMap(StreamUpdateContext::existingStreamErrorDetails)
                .map(StreamErrorOccurrence::hash)
                .map(hash -> Objects.equals(newStreamError.streamErrorOccurrence().hash(), hash))
                .orElse(false);
    }

    private boolean isErrorSameAsBefore(final StreamError newStreamError, final UUID existingErrorId) {
        return streamErrorRepository.findByErrorId(existingErrorId)
                        .map(StreamError::streamErrorOccurrence)
                        .map(StreamErrorOccurrence::hash)
                        .map(hash -> Objects.equals(newStreamError.streamErrorOccurrence().hash(), hash))
                        .orElse(false);
    }
}
