package uk.gov.justice.services.eventstore.management.catchup.observers;

import static java.lang.String.format;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.catchup.process.CatchupDurationCalculator;
import uk.gov.justice.services.eventstore.management.catchup.process.CatchupInProgress;
import uk.gov.justice.services.eventstore.management.catchup.process.EventCatchupRunner;
import uk.gov.justice.services.eventstore.management.catchup.state.CatchupError;
import uk.gov.justice.services.eventstore.management.catchup.state.CatchupErrorStateManager;
import uk.gov.justice.services.eventstore.management.catchup.state.CatchupStateManager;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupCompletedForSubscriptionEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupProcessingOfEventFailedEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupRequestedEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupStartedEvent;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;
import uk.gov.justice.services.jmx.logging.MdcLoggerInterceptor;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.interceptor.Interceptors;

import org.slf4j.Logger;

@Interceptors(MdcLoggerInterceptor.class)
@ApplicationScoped
public class CatchupObserver {

    @Inject
    private EventCatchupRunner eventCatchupRunner;

    @Inject
    private CatchupStateManager catchupStateManager;

    @Inject
    private CatchupErrorStateManager catchupErrorStateManager;

    @Inject
    private CatchupDurationCalculator catchupDurationCalculator;

    @Inject
    private Event<SystemCommandStateChangedEvent> systemCommandStateChangedEventFirer;

    @Inject
    private CatchupProcessCompleter catchupProcessCompleter;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    public void onCatchupRequested(@Observes final CatchupRequestedEvent catchupRequestedEvent) {
        final UUID commandId = catchupRequestedEvent.getCommandId();
        final CatchupCommand catchupCommand = catchupRequestedEvent.getCatchupCommand();
        final Optional<UUID> runFromEventId = catchupRequestedEvent.getRunFromEventId();

        final ZonedDateTime catchupStartedAt = clock.now();

        catchupStateManager.clear();
        catchupErrorStateManager.clear(catchupCommand);

        final String message = format("%s requested at %s", catchupCommand.getName(), catchupStartedAt);
        systemCommandStateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                commandId,
                catchupCommand,
                COMMAND_IN_PROGRESS,
                catchupStartedAt,
                message
        ));

        logger.info(message);

        eventCatchupRunner.runEventCatchup(commandId, catchupCommand, runFromEventId);
    }

    public void onCatchupStarted(@Observes final CatchupStartedEvent catchupStartedEvent) {

        final List<SubscriptionCatchupDetails> subscriptionCatchupDetailsList = catchupStartedEvent
                .getSubscriptionCatchupDefinition();

        final ZonedDateTime catchupStartedAt = catchupStartedEvent.getCatchupStartedAt();
        final CatchupCommand catchupCommand = catchupStartedEvent.getCatchupCommand();

        catchupStateManager.newCatchupInProgress(
                subscriptionCatchupDetailsList,
                catchupStartedAt);

        logger.info(format("%s started at %s", catchupCommand.getName(), catchupStartedAt));
    }

    public void onCatchupCompleteForSubscription(@Observes final CatchupCompletedForSubscriptionEvent catchupCompletedForSubscriptionEvent) {
        final UUID commandId = catchupCompletedForSubscriptionEvent.getCommandId();
        final String subscriptionName = catchupCompletedForSubscriptionEvent.getSubscriptionName();
        final String eventSourceName = catchupCompletedForSubscriptionEvent.getEventSourceName();
        final String componentName = catchupCompletedForSubscriptionEvent.getComponentName();

        final ZonedDateTime catchupCompletedAt = catchupCompletedForSubscriptionEvent.getCatchupCompletedAt();
        final int totalNumberOfEvents = catchupCompletedForSubscriptionEvent.getTotalNumberOfEvents();
        final CatchupCommand catchupCommand = catchupCompletedForSubscriptionEvent.getCatchupCommand();

        final SubscriptionCatchupDetails subscriptionCatchupDefinition = new SubscriptionCatchupDetails(subscriptionName, eventSourceName, componentName);

        logger.info(format("%s for '%s' '%s' completed at %s", catchupCommand.getName(),  componentName, subscriptionName, catchupCompletedAt));
        logger.info(format("%s for '%s' '%s' caught up %d events", catchupCommand.getName(),  componentName, subscriptionName, totalNumberOfEvents));

        final CatchupInProgress catchupInProgress = catchupStateManager.removeCatchupInProgress(subscriptionCatchupDefinition);

        final Duration catchupDuration = catchupDurationCalculator.calculate(
                catchupInProgress.getStartedAt(),
                catchupCompletedForSubscriptionEvent.getCatchupCompletedAt());

        logger.info(format("%s for '%s' '%s' took %d milliseconds", catchupCommand.getName(), componentName, subscriptionName, catchupDuration.toMillis()));

        if (catchupStateManager.noCatchupsInProgress()) {
            catchupProcessCompleter.handleCatchupComplete(commandId, catchupCommand);
        }
    }

    public void onCatchupProcessingOfEventFailed(@Observes final CatchupProcessingOfEventFailedEvent catchupProcessingOfEventFailedEvent) {
        final CatchupCommand catchupCommand = catchupProcessingOfEventFailedEvent.getCatchupCommand();

        final CatchupError catchupError = new CatchupError(
                catchupProcessingOfEventFailedEvent.getMessage(),
                catchupProcessingOfEventFailedEvent.getSubscriptionName(),
                catchupCommand,
                catchupProcessingOfEventFailedEvent.getException()
        );

        catchupErrorStateManager.add(catchupError, catchupCommand);
    }
}
