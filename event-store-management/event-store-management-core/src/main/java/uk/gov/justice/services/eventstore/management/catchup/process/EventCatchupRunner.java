package uk.gov.justice.services.eventstore.management.catchup.process;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task.EventProcessingFailedHandler;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupStartedEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.event.Event;
import javax.inject.Inject;

public class EventCatchupRunner {

    @Inject
    private EventCatchupByComponentRunner eventCatchupByComponentRunner;

    @Inject
    private Event<CatchupStartedEvent> catchupStartedEventFirer;

    @Inject
    private SubscriptionCatchupProvider subscriptionCatchupProvider;

    @Inject
    private UtcClock clock;

    @Inject
    private EventProcessingFailedHandler eventProcessingFailedHandler;

    public void runEventCatchup(
            final UUID commandId,
            final CatchupCommand catchupCommand,
            final Optional<UUID> runFromEventId) {

        final List<SubscriptionCatchupDetails> subscriptionCatchupDefinitions = subscriptionCatchupProvider.getBySubscription(catchupCommand);

        catchupStartedEventFirer.fire(new CatchupStartedEvent(
                commandId,
                catchupCommand,
                subscriptionCatchupDefinitions,
                clock.now()
        ));

        subscriptionCatchupDefinitions
                .forEach(subscriptionCatchupDetails ->
                        catchupSubscription(subscriptionCatchupDetails, commandId, catchupCommand, runFromEventId));
    }

    private void catchupSubscription(
            final SubscriptionCatchupDetails subscriptionCatchupDetails,
            final UUID commandId,
            final CatchupCommand catchupCommand,
            final Optional<UUID> runFromEventId) {

        try {
            eventCatchupByComponentRunner.runEventCatchupForComponent(
                    subscriptionCatchupDetails,
                    commandId,
                    catchupCommand,
                    runFromEventId);
        } catch (final Exception e) {
            eventProcessingFailedHandler.handleSubscriptionFailure(e, subscriptionCatchupDetails.getSubscriptionName(), commandId, catchupCommand);
        }
    }
}
