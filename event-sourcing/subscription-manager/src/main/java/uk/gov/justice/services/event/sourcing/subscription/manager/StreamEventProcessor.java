package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static jakarta.transaction.Transactional.TxType.NOT_SUPPORTED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingException;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

public class StreamEventProcessor {

    @Inject
    private ComponentEventProcessor componentEventProcessor;

    @Inject
    private StreamErrorStatusHandler streamErrorStatusHandler;

    @Inject
    private NextEventSelector nextEventSelector;

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

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

        transactionHandler.begin();

        final Optional<PulledEvent> pulledEvent = selectEvent(source, component);

        if (pulledEvent.isEmpty()) {
            transactionHandler.rollback();
            return EVENT_NOT_FOUND;
        }

        final UUID streamId = pulledEvent.get().lockedStreamStatus().streamId();

        try (final StreamSessionLockManager.StreamSessionLock lock = streamSessionLockManager.lockStream(streamId, source, component)) {
            if (lock.isAcquired()) {
                processEventFound(pulledEvent.get(), source, component);
            }else {
                transactionHandler.rollback();
            }
            return EVENT_FOUND;
        }
    }

    private void processEventFound(final PulledEvent pulledEvent, final String source, final String component) {
        final JsonEnvelope eventJsonEnvelope = pulledEvent.jsonEnvelope();
        final LockedStreamStatus lockedStreamStatus = pulledEvent.lockedStreamStatus();
        final long streamCurrentPosition = lockedStreamStatus.position();

        try {
            streamEventLoggerMetadataAdder.addRequestDataToMdc(eventJsonEnvelope, component);
            streamEventValidator.validate(eventJsonEnvelope, source, component);

            componentEventProcessor.process(eventJsonEnvelope, component);

            updateStreamPosition(lockedStreamStatus, source, component, getEventPosition(eventJsonEnvelope));

            streamErrorStatusHandler.onStreamProcessingSuccess(
                    lockedStreamStatus.streamId(), source, component, lockedStreamStatus.streamErrorId());

            micrometerMetricsCounters.incrementEventsSucceededCount(source, component);
            transactionHandler.commit();
        } catch (final Exception e) {
            transactionHandler.rollback();
            micrometerMetricsCounters.incrementEventsFailedCount(source, component);
            streamErrorStatusHandler.onStreamProcessingFailure(
                    eventJsonEnvelope, e, component, streamCurrentPosition, lockedStreamStatus.streamErrorId());
        } finally {
            streamEventLoggerMetadataAdder.clearMdc();
        }
    }

    private Optional<PulledEvent> selectEvent(final String source, final String component) {
        try {
            final Optional<LockedStreamStatus> lockedStreamStatusOpt = newStreamStatusRepository
                    .findOldestStreamToProcessByAcquiringLock(source, component, streamProcessingConfig.getMaxRetries());
            return nextEventSelector.selectNextEvent(source, lockedStreamStatusOpt);
        } catch (final StreamProcessingException e) {
            transactionHandler.rollback();
            throw e;
        } catch (final Exception e) {
            transactionHandler.rollback();
            throw new StreamProcessingException(
                    format("Failed to select event for processing: source '%s', component '%s'", source, component), e);
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

}