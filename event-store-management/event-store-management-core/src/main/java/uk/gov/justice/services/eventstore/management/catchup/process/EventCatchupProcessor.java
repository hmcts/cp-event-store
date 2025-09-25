package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.NEVER;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager.ConcurrentEventStreamConsumerManager;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupCompletedForSubscriptionEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.util.UUID;
import java.util.stream.Stream;

import javax.enterprise.event.Event;
import javax.inject.Inject;
import javax.transaction.Transactional;

import org.slf4j.Logger;

public class EventCatchupProcessor {

    @Inject
    private ConcurrentEventStreamConsumerManager concurrentEventStreamConsumerManager;

    @Inject
    private MissingEventStreamer missingEventStreamer;

    @Inject
    private Event<CatchupCompletedForSubscriptionEvent> catchupCompletedForSubscriptionEventFirer;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    @Transactional(NEVER)
    public void performEventCatchup(final CatchupSubscriptionContext catchupSubscriptionContext) {

        final UUID commandId = catchupSubscriptionContext.getCommandId();
        final SubscriptionCatchupDetails subscriptionCatchupDefinition = catchupSubscriptionContext.getSubscriptionCatchupDefinition();
        final String subscriptionName = subscriptionCatchupDefinition.getSubscriptionName();
        final String eventSourceName = subscriptionCatchupDefinition.getEventSourceName();
        final String componentName = catchupSubscriptionContext.getComponentName();
        final CatchupCommand catchupCommand = catchupSubscriptionContext.getCatchupCommand();

        logger.info(format("Finding all missing events for event source '%s', component '%s", eventSourceName, componentName));
        final Stream<LinkedEvent> events = missingEventStreamer.getMissingEvents(eventSourceName, componentName);

        final int totalEventsProcessed = events.mapToInt(event -> {

            final Long eventNumber = event.getEventNumber().orElseThrow(() -> new MissingEventNumberException(format("PublishedEvent with id '%s' is missing its event number", event.getId())));

            // here

            if (eventNumber % 1000L == 0) {
                logger.info(format("%s with Event Source: %s for Event Number: %d", catchupCommand.getName(), eventSourceName, eventNumber));
            }

            return concurrentEventStreamConsumerManager.add(event, subscriptionName, catchupCommand, commandId);

        }).sum();

        logger.info(format("%d active PublishedEvents queued for publishing", totalEventsProcessed));
        logger.info("Waiting for publishing consumer completion...");
        concurrentEventStreamConsumerManager.waitForCompletion();

        final CatchupCompletedForSubscriptionEvent event = new CatchupCompletedForSubscriptionEvent(
                commandId,
                subscriptionName,
                eventSourceName,
                componentName,
                catchupCommand,
                clock.now(),
                totalEventsProcessed);

        catchupCompletedForSubscriptionEventFirer.fire(event);
    }
}
