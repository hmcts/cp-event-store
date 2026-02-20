package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.Optional;
import java.util.UUID;
import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;
import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamErrorStatusHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamRetryStatusManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NextEventSelector.PulledEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_NOT_FOUND;

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
    private StreamRetryStatusManager streamRetryStatusManager;

    @Inject
    private StreamErrorRepository streamErrorRepository;

    @Transactional(value = NOT_SUPPORTED)
    public EventProcessingStatus processSingleEvent(final String source, final String component) {
        micrometerMetricsCounters.incrementEventsProcessedCount(source, component);

        transactionHandler.begin(userTransaction);

        final Optional<LockedStreamStatus> lockedStreamStatusOpt = streamSelectorManager.selectStreamToProcess(source, component);
        final Optional<PulledEvent> pulledEvent = nextEventSelector.selectNextEvent(source, component, lockedStreamStatusOpt);

        if (pulledEvent.isPresent()) {
            final JsonEnvelope eventJsonEnvelope = pulledEvent.get().jsonEnvelope();
            final LockedStreamStatus lockedStreamStatus = pulledEvent.get().lockedStreamStatus();
            final UUID streamId = lockedStreamStatus.streamId();
            final long latestKnownPosition = lockedStreamStatus.latestKnownPosition();
            final long streamCurrentPosition = lockedStreamStatus.position();
            final Metadata metadata = eventJsonEnvelope.metadata();

            try {
                streamEventLoggerMetadataAdder.addRequestDataToMdc(eventJsonEnvelope, component);
                final long eventPositionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", metadata.name(), metadata.id())));

                streamEventValidator.validate(eventJsonEnvelope, source, component);

                componentEventProcessor.process(eventJsonEnvelope, component);

                newStreamStatusRepository.updateCurrentPosition(streamId, source, component, eventPositionInStream);

                if (latestKnownPosition == eventPositionInStream) {
                    newStreamStatusRepository.setUpToDate(true, streamId, source, component);
                }

                streamRetryStatusManager.removeStreamRetryStatus(streamId, source, component);

                lockedStreamStatus.streamErrorId().ifPresent(
                        streamErrorId ->
                                streamErrorRepository.markStreamAsFixed(streamErrorId, streamId, source, component));

                micrometerMetricsCounters.incrementEventsSucceededCount(source, component);
                transactionHandler.commit(userTransaction);

                return EVENT_FOUND;

            } catch (final Exception e) {
                transactionHandler.rollback(userTransaction);
                micrometerMetricsCounters.incrementEventsFailedCount(source, component);
                streamErrorStatusHandler.onStreamProcessingFailure(eventJsonEnvelope, e, component, streamCurrentPosition, lockedStreamStatus.streamErrorId());
                return EVENT_FOUND;
            } finally {
                streamEventLoggerMetadataAdder.clearMdc();
            }
        } else {
            commitWithFallBackToRollback();
            return EVENT_NOT_FOUND;
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
