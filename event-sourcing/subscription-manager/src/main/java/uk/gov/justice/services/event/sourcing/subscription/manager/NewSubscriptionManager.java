package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPoisitionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.subscription.SubscriptionManager;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public class NewSubscriptionManager implements SubscriptionManager  {

    private final NewEventBufferManager newEventBufferManager;
    private final NewStreamStatusRepository newStreamStatusRepository;
    private final EventProcessingStatusCalculator eventProcessingStatusCalculator;
    private final NewEventBufferProcessor newEventBufferProcessor;
    private final StreamStatusManager streamStatusManager;
    private final String componentName;

    public NewSubscriptionManager(
            final NewEventBufferManager newEventBufferManager,
            final NewStreamStatusRepository newStreamStatusRepository,
            final EventProcessingStatusCalculator eventProcessingStatusCalculator,
            final NewEventBufferProcessor newEventBufferProcessor,
            final StreamStatusManager streamStatusManager,
            final String componentName) {
        this.newEventBufferManager = newEventBufferManager;
        this.newStreamStatusRepository = newStreamStatusRepository;
        this.eventProcessingStatusCalculator = eventProcessingStatusCalculator;
        this.newEventBufferProcessor = newEventBufferProcessor;
        this.streamStatusManager = streamStatusManager;
        this.componentName = componentName;
    }

    @Override
    public void process(final JsonEnvelope incomingJsonEnvelope) {

        final Metadata metadata = incomingJsonEnvelope.metadata();
        final String name = metadata.name();
        final UUID eventId = metadata.id();
        final UUID streamId = metadata.streamId().orElseThrow(() -> new MissingStreamIdException(format("No streamId found in event: name '%s', eventId '%s'", name, eventId)));
        final String source = metadata.source().orElseThrow(() -> new MissingSourceException(format("No source found in event: name '%s', eventId '%s'", name, eventId)));
        final Long incomingPositionInStream = metadata.position().orElseThrow(() -> new MissingPoisitionInStreamException(format("No position found in event: name '%s', eventId '%s'", name, eventId)));

        final ZonedDateTime updatedAt = new UtcClock().now();

        streamStatusManager.createNewStreamInStreamStatusTableIfNecessary(
                streamId,
                source,
                componentName,
                updatedAt,
                false);

        final StreamPositions streamPositions = newStreamStatusRepository.lockRowAndGetPosition(
                streamId,
                source,
                componentName,
                incomingPositionInStream);

        final EventProcessingStatus eventProcessingStatus = eventProcessingStatusCalculator
                .calculateProcessingStatus(streamPositions);

        switch (eventProcessingStatus) {
            case EVENT_IS_OBSOLETE -> {
                // don't care (maybe log???)
            }
            case SHOULD_BUFFER_EVENT -> {
                newEventBufferManager.addToBuffer(incomingJsonEnvelope, componentName);
            }
            case SHOULD_PROCESS_EVENT -> {
                newEventBufferProcessor.process(incomingJsonEnvelope, componentName);
            }
        }
    }
}
