package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.util.Queue;
import java.util.UUID;

import jakarta.annotation.Resource;
import jakarta.enterprise.concurrent.ManagedExecutorService;
import jakarta.inject.Inject;

public class ConsumeEventQueueTaskManager {

    @Resource
    private ManagedExecutorService managedExecutorService;

    @Inject
    private ConsumeEventQueueTaskFactory consumeEventQueueTaskFactory;

    public void consume(
            final Queue<LinkedEvent> events,
            final String subscriptionName,
            final CatchupCommand catchupCommand,
            final UUID commandId) {


        final ConsumeEventQueueTask consumeEventQueueTask = consumeEventQueueTaskFactory.createConsumeEventQueueTask(
                events,
                subscriptionName,
                catchupCommand,
                commandId
        );

        managedExecutorService.execute(consumeEventQueueTask);
    }
}
