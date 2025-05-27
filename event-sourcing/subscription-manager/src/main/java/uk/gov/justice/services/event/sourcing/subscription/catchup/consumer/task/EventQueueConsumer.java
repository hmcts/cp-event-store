package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.CatchupEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventStreamConsumptionResolver;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.FinishedProcessingMessage;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import javax.inject.Inject;
import java.util.Queue;
import java.util.UUID;

public class EventQueueConsumer {

    @Inject
    private CatchupEventProcessor catchupEventProcessor;

    @Inject
    private EventStreamConsumptionResolver eventStreamConsumptionResolver;

    @Inject
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    public boolean consumeEventQueue(
            final UUID commandId,
            final Queue<PublishedEvent> events,
            final String subscriptionName,
            final CatchupCommand catchupCommand) {

        while (!events.isEmpty()) {

            final PublishedEvent publishedEvent = events.poll();
            try {
                catchupEventProcessor.processWithEventBuffer(publishedEvent, subscriptionName);
            } catch (final Exception e) {
                eventProcessingFailedHandler.handleEventFailure(e, publishedEvent, subscriptionName, catchupCommand, commandId);
            } finally {
                eventStreamConsumptionResolver.decrementEventsInProcessCount();
            }
        }

        return eventStreamConsumptionResolver.isEventConsumptionComplete(new FinishedProcessingMessage(events));
    }
}
