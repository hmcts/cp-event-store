package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static jakarta.ejb.TransactionManagementType.CONTAINER;
import static jakarta.transaction.Transactional.TxType.NEVER;

import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.EventStreamConsumptionResolver;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupProcessingOfEventFailedEvent;

import java.util.Queue;
import java.util.UUID;

import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;
import jakarta.ejb.TransactionAttributeType;
import jakarta.ejb.TransactionManagement;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@Stateless
@TransactionManagement(CONTAINER)
@TransactionAttribute(value = TransactionAttributeType.NEVER)
public class ConsumeEventQueueBean {

    @Inject
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    @Inject
    private Event<CatchupProcessingOfEventFailedEvent> catchupProcessingOfEventFailedEventFirer;

    @Inject
    private EventStreamConsumptionResolver eventStreamConsumptionResolver;

    @Inject
    private EventQueueConsumer eventQueueConsumer;

    @Transactional(NEVER)
    public void consume(
            final Queue<LinkedEvent> events,
            final String subscriptionName,
            final CatchupCommand catchupCommand,
            final UUID commandId) {

        boolean consumed = false;
        while (!consumed) {
            try {
                consumed = eventQueueConsumer.consumeEventQueue(
                        commandId,
                        events,
                        subscriptionName,
                        catchupCommand);
            } catch (final Exception e) {
                eventStreamConsumptionResolver.decrementEventsInProcessCountBy(events.size());
                events.clear();
                eventProcessingFailedHandler.handleStreamFailure(e, subscriptionName, catchupCommand, commandId);
            }
        }
    }
}
