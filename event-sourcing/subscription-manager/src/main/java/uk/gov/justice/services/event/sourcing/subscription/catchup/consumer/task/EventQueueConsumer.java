package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.CatchupEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventStreamConsumptionResolver;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.FinishedProcessingMessage;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.util.Queue;
import java.util.UUID;

import javax.inject.Inject;

public class EventQueueConsumer {

    @Inject
    private CatchupEventProcessor catchupEventProcessor;

    @Inject
    private EventStreamConsumptionResolver eventStreamConsumptionResolver;

    @Inject
    private LinkedEventMetadataUpdater linkedEventMetadataUpdater;

    @Inject
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    public boolean consumeEventQueue(
            final UUID commandId,
            final Queue<LinkedEvent> events,
            final String subscriptionName,
            final CatchupCommand catchupCommand) {

        while (!events.isEmpty()) {

            final LinkedEvent linkedEvent = events.poll();
            try {
                final LinkedEvent linkedEventWithUpdatedMetadata = linkedEventMetadataUpdater.addEventNumbersToMetadataOf(linkedEvent);
                catchupEventProcessor.processWithEventBuffer(linkedEventWithUpdatedMetadata, subscriptionName);
            } catch (final Exception e) {
                eventProcessingFailedHandler.handleEventFailure(e, linkedEvent, subscriptionName, catchupCommand, commandId);
            } finally {
                eventStreamConsumptionResolver.decrementEventsInProcessCount();
            }
        }

        return eventStreamConsumptionResolver.isEventConsumptionComplete(new FinishedProcessingMessage(events));
    }
}
