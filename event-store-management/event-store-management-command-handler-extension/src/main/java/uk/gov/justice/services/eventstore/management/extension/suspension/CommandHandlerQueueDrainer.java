package uk.gov.justice.services.eventstore.management.extension.suspension;

import static java.lang.String.format;
import static uk.gov.justice.services.management.suspension.api.SuspensionResult.suspensionFailed;
import static uk.gov.justice.services.management.suspension.api.SuspensionResult.suspensionSucceeded;

import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;
import uk.gov.justice.services.management.suspension.api.Suspendable;
import uk.gov.justice.services.management.suspension.api.SuspensionResult;
import uk.gov.justice.services.management.suspension.commands.SuspensionCommand;

import java.util.UUID;

import jakarta.inject.Inject;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

public class CommandHandlerQueueDrainer implements Suspendable {

    @Inject
    private CommandHandlerQueueInterrogator commandHandlerQueueInterrogator;

    @Inject
    private StopWatchFactory stopWatchFactory;

    @Inject
    private Logger logger;

    @Override
    public boolean shouldSuspend() {
        return true;
    }

    @Override
    public SuspensionResult suspend(final UUID commandId, final SuspensionCommand applicationShutteringCommand) {

        logger.info("Shuttering Command Handler. Waiting for queue to drain");

        final StopWatch stopWatch = stopWatchFactory.createStartedStopWatch();

        try {
            final boolean queueEmpty = commandHandlerQueueInterrogator.pollUntilEmptyHandlerQueue();

            if (queueEmpty) {

                final String message = "Command Handler Queue drained successfully";
                logger.info(message);

                return suspensionSucceeded(
                        getName(),
                        commandId,
                        message,
                        applicationShutteringCommand
                );
            }

            stopWatch.stop();

            final String message = format("Failed to drain command handler queue in %d milliseconds", stopWatch.getTime());
            logger.error(message);

            return suspensionFailed(
                    getName(),
                    commandId,
                    message,
                    applicationShutteringCommand
            );
        } finally {
            stopWatch.stop();
        }
    }
}
