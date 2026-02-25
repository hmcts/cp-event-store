package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler.SavepointContext;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

public class StreamEventProcessor {

    @Inject
    private ComponentEventProcessor componentEventProcessor;

    @Inject
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Inject
    private OldestStreamSelector oldestStreamSelector;

    @Inject
    private NextEventSelector nextEventSelector;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Inject
    private StreamEventValidator streamEventValidator;

    @Inject
    private Logger logger;

    @Transactional(value = NOT_SUPPORTED)
    public EventProcessingStatus processSingleEvent(final String source, final String component) {
        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);

        transactionHandler.begin();

        final Optional<LockedStreamStatus> lockedStreamStatusOpt;
        final Optional<PulledEvent> pulledEvent;

        try {
            lockedStreamStatusOpt = oldestStreamSelector.findStreamToProcess(source, component);
            pulledEvent = nextEventSelector.selectNextEvent(source, component, lockedStreamStatusOpt);
        } catch (final Exception e) {
            transactionHandler.rollback();
            throw e;
        }

        if (pulledEvent.isPresent()) {
            processEvent(pulledEvent.get(), source, component);
            return EVENT_FOUND;
        } else {
            transactionHandler.commitWithFallbackToRollback();
            return EVENT_NOT_FOUND;
        }
    }

    private void processEvent(final PulledEvent pulledEvent, final String source, final String component) {
        final JsonEnvelope eventJsonEnvelope = pulledEvent.jsonEnvelope();
        final LockedStreamStatus lockedStreamStatus = pulledEvent.lockedStreamStatus();
        final UUID streamId = lockedStreamStatus.streamId();
        final long latestKnownPosition = lockedStreamStatus.latestKnownPosition();
        final long streamCurrentPosition = lockedStreamStatus.position();
        final Metadata metadata = eventJsonEnvelope.metadata();

        final SavepointContext ctx;
        try {
            ctx = transactionHandler.createSavepointContext();
        } catch (final SQLException e) {
            logger.error("Failed to create savepoint context for streamId '{}': {}", streamId, e.getMessage(), e);
            transactionHandler.rollback();
            return;
        }

        try {
            streamEventLoggerMetadataAdder.addRequestDataToMdc(eventJsonEnvelope, component);
            final long eventPositionInStream = metadata.position().orElseThrow(
                    () -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));

            streamEventValidator.validate(eventJsonEnvelope, source, component);

            componentEventProcessor.process(eventJsonEnvelope, component);

            newStreamStatusRepository.updateCurrentPosition(streamId, source, component, eventPositionInStream);
            if (latestKnownPosition == eventPositionInStream) {
                newStreamStatusRepository.setUpToDate(true, streamId, source, component);
            }

            lockedStreamStatus.streamErrorId().ifPresent(streamErrorId -> streamErrorStatusHandler.markStreamAsFixed(streamErrorId, streamId, source, component));

            transactionHandler.releaseSavepointAndCommit(ctx);
            micrometerMetricsCounters.incrementEventsSucceededCount(source, component);

        } catch (final Exception e) {
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            transactionHandler.rollbackSavepointAndRestartIfTainted(ctx);
            recordStreamErrorSafely(eventJsonEnvelope, e, component, streamCurrentPosition, lockedStreamStatus, streamId);

        } finally {
            streamEventLoggerMetadataAdder.clearMdc();
            transactionHandler.closeSavepointContext(ctx);
        }
    }

    private void recordStreamErrorSafely(final JsonEnvelope eventJsonEnvelope, final Exception cause, final String component,
            final long streamCurrentPosition, final LockedStreamStatus lockedStreamStatus, final UUID streamId) {
        try {
            streamErrorStatusHandler.recordStreamError(eventJsonEnvelope, cause, component, streamCurrentPosition, lockedStreamStatus.streamErrorId());
            transactionHandler.commit();
        } catch (final Exception errorException) {
            logger.error("Failed to record stream error for streamId '{}': {}", streamId, errorException.getMessage(), errorException);
            transactionHandler.rollback();
        }
    }
}