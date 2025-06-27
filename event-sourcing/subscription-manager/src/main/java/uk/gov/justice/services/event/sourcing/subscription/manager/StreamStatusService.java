package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusException;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.metrics.micrometer.counters.MicrometerMetricsCounters;

import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

public class StreamStatusService {

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    @Inject
    private EventProcessingStatusCalculator eventProcessingStatusCalculator;

    @Inject
    private NewEventBufferManager newEventBufferManager;

    @Inject
    private LatestKnownPositionAndIsUpToDateUpdater latestKnownPositionAndIsUpToDateUpdater;

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private TransactionHandler transactionHandler;

    @Inject
    private MicrometerMetricsCounters micrometerMetricsCounters;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    @Inject
    private EventSourceNameCalculator eventSourceNameCalculator;

    @Transactional(NOT_SUPPORTED)
    public EventOrderingStatus handleStreamStatusUpdates(final JsonEnvelope incomingJsonEnvelope, final String component) {

        final Metadata metadata = incomingJsonEnvelope.metadata();
        final String name = metadata.name();
        final UUID eventId = metadata.id();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(format("No streamId found in event: name '%s', eventId '%s'", name, eventId)));
        final String source = eventSourceNameCalculator.getSource(incomingJsonEnvelope);
        final Long incomingPositionInStream = metadata.position().orElseThrow(() -> new MissingPositionInStreamException(format("No position found in event: name '%s', eventId '%s'", name, eventId)));

        try {
            transactionHandler.begin(userTransaction);

            newStreamStatusRepository.insertIfNotExists(
                    streamId,
                    source,
                    component,
                    clock.now(),
                    false);

            final StreamPositions streamPositions = newStreamStatusRepository.lockRowAndGetPositions(
                    streamId,
                    source,
                    component,
                    incomingPositionInStream);

            latestKnownPositionAndIsUpToDateUpdater.updateIfNecessary(
                    streamPositions,
                    streamId,
                    source,
                    component);

            final EventOrderingStatus eventOrderingStatus = eventProcessingStatusCalculator.calculateEventOrderingStatus(streamPositions);

            switch (eventOrderingStatus) {
                case EVENT_OUT_OF_ORDER ->
                        newEventBufferManager.addToBuffer(incomingJsonEnvelope, component);
                case EVENT_ALREADY_PROCESSED -> {
                    micrometerMetricsCounters.incrementEventsIgnoredCount(source, component);
                    logger.info(format("Duplicate incoming event detected. Event already processed; ignoring. eventId: '%s', streamId: '%s', incomingEventPositionInStream '%d' currentStreamPosition: '%d'",
                            eventId,
                            streamId,
                            streamPositions.incomingEventPosition(),
                            streamPositions.currentStreamPosition()));
                }
            }

            transactionHandler.commit(userTransaction);

            return eventOrderingStatus;

        } catch (final Exception e) {
            transactionHandler.rollback(userTransaction);
            throw new StreamStatusException("Failed to update stream_status/event_buffer for eventId '%s', eventName '%s', streamId '%s'".formatted(eventId, name, streamId), e);
        }
    }
}
