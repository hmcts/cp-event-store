package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.util.Queue;
import java.util.UUID;

import jakarta.inject.Inject;

public class ConsumeEventQueueTaskFactory {

    @Inject
    private ConsumeEventQueueBean consumeEventQueueBean;

    public ConsumeEventQueueTask createConsumeEventQueueTask(
            final Queue<LinkedEvent> events,
            final String subscriptionName,
            final CatchupCommand catchupCommand,
            final UUID commandId) {

        return new ConsumeEventQueueTask(
                consumeEventQueueBean,
                events,
                subscriptionName,
                catchupCommand,
                commandId
        );
    }
}
