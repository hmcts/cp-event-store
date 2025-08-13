package uk.gov.justice.services.event.sourcing.subscription.error;

import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamError;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamErrorDetails;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import javax.inject.Inject;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;

public class StreamErrorStatusHandler {

    @Inject
    private ExceptionDetailsRetriever exceptionDetailsRetriever;

    @Inject
    private StreamErrorConverter streamErrorConverter;

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private Logger logger;

    public void onStreamProcessingFailure(final JsonEnvelope jsonEnvelope, final Throwable exception, final String source, final String component, final StreamUpdateContext streamUpdateContext) {

        micrometerMetricsCounters.incrementEventsFailedCount(source, component);

        final ExceptionDetails exceptionDetails = exceptionDetailsRetriever.getExceptionDetailsFrom(exception);
        final StreamError newStreamError = streamErrorConverter.asStreamError(exceptionDetails, jsonEnvelope, component);
        try {
            transactionHandler.begin(userTransaction);

            if (isErrorSameAsBefore(newStreamError, streamUpdateContext))
                streamErrorRepository.markSameErrorHappened(newStreamError, streamUpdateContext.currentStreamPosition(), streamUpdateContext.lastUpdatedAt());
            else {
                streamErrorRepository.markStreamAsErrored(newStreamError, streamUpdateContext.currentStreamPosition());
            }

            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            logger.error("Failed to mark stream as errored: streamId '%s'".formatted(newStreamError.streamErrorDetails().streamId()), e);
        }
    }

    private boolean isErrorSameAsBefore(final StreamError newStreamError, final StreamUpdateContext streamUpdateContext) {
        return Optional.of(streamUpdateContext)
                .flatMap(StreamUpdateContext::existingStreamErrorDetails)
                .map(StreamErrorDetails::hash)
                .map(hash -> Objects.equals(newStreamError.streamErrorDetails().hash(), hash))
                .orElse(false);
    }
}
