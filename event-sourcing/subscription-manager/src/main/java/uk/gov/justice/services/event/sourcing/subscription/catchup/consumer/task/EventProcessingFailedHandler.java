package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupProcessingOfEventFailedEvent;

import java.util.UUID;

import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.slf4j.Logger;

public class EventProcessingFailedHandler {

    @Inject
    private Event<CatchupProcessingOfEventFailedEvent> catchupProcessingOfEventFailedEventFirer;

    @Inject
    private Logger logger;

    public void handleEventFailure(
            final Exception exception,
            final LinkedEvent linkedEvent,
            final String subscriptionName,
            final CatchupCommand catchupCommand,
            final UUID commandId) {

        final String logMessage = format(
                "Failed to process publishedEvent: name: '%s', id: '%s', streamId: '%s'",
                linkedEvent.getName(),
                linkedEvent.getId(),
                linkedEvent.getStreamId()
        );

        handleFailure(catchupCommand, commandId, logMessage, subscriptionName, exception);
    }

    public void handleStreamFailure(
            final Exception exception,
            final String subscriptionName,
            final CatchupCommand catchupCommand,
            final UUID commandId) {

        final String logMessage = "Failed to consume stream of events. Aborting...";

        handleFailure(catchupCommand, commandId, logMessage, subscriptionName, exception);
    }

    public void handleSubscriptionFailure(
            final Exception exception,
            final String subscriptionName,
            final UUID commandId,
            final CatchupCommand catchupCommand) {

        final String logMessage = String.format("Failed to subscribe to '%s'. Aborting...", subscriptionName);

        handleFailure(catchupCommand, commandId, logMessage, subscriptionName, exception);
    }

    private void handleFailure(final CatchupCommand catchupCommand, final UUID commandId, final String logMessage, final String subscriptionName, final Exception exception) {
        logger.error(
                logMessage,
                exception);

        final String errorMessage = format("%s: %s: %s", logMessage, exception.getClass().getSimpleName(), exception.getMessage());

        final CatchupProcessingOfEventFailedEvent catchupProcessingOfEventFailedEvent = new CatchupProcessingOfEventFailedEvent(
                commandId,
                errorMessage,
                exception,
                catchupCommand,
                subscriptionName
        );

        catchupProcessingOfEventFailedEventFirer.fire(catchupProcessingOfEventFailedEvent);
    }
}
