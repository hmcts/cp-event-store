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
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.sql.Connection;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;

public class StreamEventProcessor {

    @Inject
    private ComponentEventProcessor componentEventProcessor;

    @Inject
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Inject
    private StreamSelectorManager streamSelectorManager;

    @Inject
    private NextEventSelector nextEventSelector;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private StreamEventLoggerMetadataAdder streamEventLoggerMetadataAdder;

    @Inject
    private StreamEventValidator streamEventValidator;

    @Inject
    private StreamSessionLockManager streamSessionLockManager;

    @Transactional(value = NOT_SUPPORTED)
    public EventProcessingStatus processSingleEvent(final String source, final String component) {
        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);

        final Connection advisoryLockConnection = streamSessionLockManager.openLockConnection();
        UUID lockedStreamId = null;

        try {
            transactionHandler.begin(userTransaction);

            final Optional<PulledEvent> pulledEvent = selectEvent(source, component);

            if (pulledEvent.isEmpty()) {
                commitWithFallBackToRollback();
                return EVENT_NOT_FOUND;
            }

            final LockedStreamStatus lockedStreamStatus = pulledEvent.get().lockedStreamStatus();
            lockedStreamId = lockedStreamStatus.streamId();

            if (!streamSessionLockManager.tryLockStream(advisoryLockConnection, lockedStreamId)) {
                commitWithFallBackToRollback();
                return EVENT_NOT_FOUND;
            }

            processEvent(pulledEvent.get(), source, component);
            return EVENT_FOUND;
        } finally {
            if (lockedStreamId != null) {
                streamSessionLockManager.unlockStream(advisoryLockConnection, lockedStreamId);
            }
            streamSessionLockManager.closeQuietly(advisoryLockConnection);
        }
    }

    private Optional<PulledEvent> selectEvent(final String source, final String component) {
        try {
            final Optional<LockedStreamStatus> lockedStreamStatusOpt = streamSelectorManager.selectStreamToProcess(source, component);
            return nextEventSelector.selectNextEvent(source, component, lockedStreamStatusOpt);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            throw e;
        }
    }

    private void processEvent(final PulledEvent pulledEvent, final String source, final String component) {
        final JsonEnvelope eventJsonEnvelope = pulledEvent.jsonEnvelope();
        final LockedStreamStatus lockedStreamStatus = pulledEvent.lockedStreamStatus();
        final long streamCurrentPosition = lockedStreamStatus.position();

        try {
            streamEventLoggerMetadataAdder.addRequestDataToMdc(eventJsonEnvelope, component);

            final long eventPositionInStream = getEventPosition(eventJsonEnvelope);

            streamEventValidator.validate(eventJsonEnvelope, source, component);
            componentEventProcessor.process(eventJsonEnvelope, component);

            updateStreamPosition(lockedStreamStatus, source, component, eventPositionInStream);

            streamErrorStatusHandler.onStreamProcessingSuccess(
                    lockedStreamStatus.streamId(), source, component, lockedStreamStatus.streamErrorId());

            micrometerMetricsCounters.incrementEventsSucceededCount(source, component);
            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            streamErrorStatusHandler.onStreamProcessingFailure(
                    eventJsonEnvelope, e, component, streamCurrentPosition, lockedStreamStatus.streamErrorId());
        } finally {
            streamEventLoggerMetadataAdder.clearMdc();
        }
    }

    private long getEventPosition(final JsonEnvelope eventJsonEnvelope) {
        final Metadata metadata = eventJsonEnvelope.metadata();
        return metadata.position().orElseThrow(() ->
                new MissingPositionInStreamException(format(
                        "No position found in event: name '%s', eventId '%s'",
                        metadata.name(), metadata.id())));
    }

    private void updateStreamPosition(final LockedStreamStatus lockedStreamStatus, final String source, final String component, final long eventPositionInStream) {
        final UUID streamId = lockedStreamStatus.streamId();
        newStreamStatusRepository.updateCurrentPosition(streamId, source, component, eventPositionInStream);

        if (lockedStreamStatus.latestKnownPosition() == eventPositionInStream) {
            newStreamStatusRepository.setUpToDate(true, streamId, source, component);
        }
    }

    private void commitWithFallBackToRollback() {
        try {
            transactionHandler.commit(userTransaction);
        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
        }
    }
}