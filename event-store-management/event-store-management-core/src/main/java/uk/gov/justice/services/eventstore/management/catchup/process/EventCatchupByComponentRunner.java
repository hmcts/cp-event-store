package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.lang.String.format;

import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

public class EventCatchupByComponentRunner {

    @Inject
    private EventCatchupProcessorBean eventCatchupProcessorBean;

    @Inject
    private RunFromEventNumberFinder runFromEventNumberFinder;

    @Inject
    private Logger logger;

    public void runEventCatchupForComponent(
            final SubscriptionCatchupDetails subscriptionCatchupDefinition,
            final UUID commandId,
            final CatchupCommand catchupCommand,
            final Optional<UUID> runFromEventId) {

        final String componentName = subscriptionCatchupDefinition.getComponentName();
        final String source = subscriptionCatchupDefinition.getSubscriptionName();
        final Long eventNumberToRunFrom = runFromEventNumberFinder.findEventNumberToRunFrom(
                runFromEventId,
                source);

        logger.info(format("Running %s for Component '%s', Subscription '%s' from event number '%d'",
                catchupCommand.getName(),
                componentName,
                source,
                eventNumberToRunFrom));

        final CatchupSubscriptionContext catchupSubscriptionContext = new CatchupSubscriptionContext(
                commandId,
                componentName,
                subscriptionCatchupDefinition,
                catchupCommand,
                eventNumberToRunFrom);

        eventCatchupProcessorBean.performEventCatchup(catchupSubscriptionContext);
    }
}
