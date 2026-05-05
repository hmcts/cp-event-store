package uk.gov.justice.services.eventstore.management.shuttering.observers;

import static java.lang.String.format;
import static uk.gov.justice.services.management.suspension.api.SuspensionResult.suspensionFailed;
import static uk.gov.justice.services.management.suspension.api.SuspensionResult.suspensionSucceeded;

import uk.gov.justice.services.eventsourcing.util.jee.timer.StopWatchFactory;
import uk.gov.justice.services.eventstore.management.shuttering.process.PublishQueueInterrogator;
import uk.gov.justice.services.management.suspension.api.Suspendable;
import uk.gov.justice.services.management.suspension.api.SuspensionResult;
import uk.gov.justice.services.management.suspension.commands.SuspensionCommand;

import java.util.UUID;

import jakarta.inject.Inject;

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;

public class PublishQueueDrainer implements Suspendable {

    @Inject
    private PublishQueueInterrogator publishQueueInterrogator;

    @Inject
    private StopWatchFactory stopWatchFactory;

    @Inject
    private Logger logger;

    @Override
    public boolean shouldSuspend() {
        return true;
    }

    @Override
    public SuspensionResult suspend(final UUID commandId, final SuspensionCommand suspensionCommand) {

        logger.info("Waiting for Publish Queue to empty");
        final StopWatch stopWatch = stopWatchFactory.createStartedStopWatch();

        try {
            final boolean queueEmpty = publishQueueInterrogator.pollUntilPublishQueueEmpty();

            if (queueEmpty) {
                final String message = "Publish Queue drained successfully";
                logger.info(message);

                return suspensionSucceeded(
                        getName(),
                        commandId,
                        message,
                        suspensionCommand
                );
            }

            stopWatch.stop();

            final String message = format("PublishQueue failed to drain after %d milliseconds", stopWatch.getTime());

            logger.error(message);

            return suspensionFailed(
                    getName(),
                    commandId,
                    message,
                    suspensionCommand
            );

        } finally {
            stopWatch.stop();
        }
    }
}
