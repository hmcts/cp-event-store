package uk.gov.justice.services.eventstore.management.verification.process;

import static java.lang.String.format;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.CommandResult;
import uk.gov.justice.services.eventstore.management.commands.VerificationCommand;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.util.UUID;

import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.slf4j.Logger;

public class EventStoreVerification {

    @Inject
    private Event<SystemCommandStateChangedEvent> systemCommandStateChangedEventFirer;

    @Inject
    private CatchupVerificationProcess catchupVerificationProcess;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    public void verifyEventStore(final UUID commandId, final VerificationCommand verificationCommand) {
        final String commandName = verificationCommand.getName();
        logger.info(format("Received %s command", commandName));

        systemCommandStateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                commandId,
                verificationCommand,
                COMMAND_IN_PROGRESS,
                clock.now(),
                format("%s command started", commandName)
        ));

        try {
            final CommandResult commandResult = catchupVerificationProcess.runVerification(commandId, verificationCommand);

            systemCommandStateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                    commandId,
                    verificationCommand,
                    commandResult.getCommandState(),
                    clock.now(),
                    commandResult.getMessage()
            ));
        } catch (final Exception e) {
            final String message = format("%s command failed: %s: %s", commandName, e.getClass().getSimpleName(), e.getMessage());

            logger.error(message, e);

            systemCommandStateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                    commandId,
                    verificationCommand,
                    COMMAND_FAILED,
                    clock.now(),
                    message
            ));
        }
    }
}
