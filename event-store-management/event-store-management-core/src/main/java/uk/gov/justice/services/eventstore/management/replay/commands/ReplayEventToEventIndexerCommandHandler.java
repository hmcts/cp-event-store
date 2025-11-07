package uk.gov.justice.services.eventstore.management.replay.commands;

import static java.util.Optional.ofNullable;
import static uk.gov.justice.services.core.annotation.Component.EVENT_INDEXER;
import static uk.gov.justice.services.eventstore.management.commands.ReplayEventToEventIndexerCommand.REPLAY_EVENT_TO_EVENT_INDEXER;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.ReplayEventToEventIndexerCommand;
import uk.gov.justice.services.eventstore.management.replay.process.ReplayEventToComponentRunner;
import uk.gov.justice.services.jmx.api.domain.CommandState;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.command.HandlesSystemCommand;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.util.Optional;
import java.util.UUID;

import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;

public class ReplayEventToEventIndexerCommandHandler {

    @Inject
    private Event<SystemCommandStateChangedEvent> stateChangedEventFirer;

    @Inject
    private ReplayEventToComponentRunner replayEventToComponentRunner;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    @HandlesSystemCommand(REPLAY_EVENT_TO_EVENT_INDEXER)
    public void replayEventToEventIndexer(
            final ReplayEventToEventIndexerCommand command,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {
        fireEvent(COMMAND_IN_PROGRESS, command, commandId, "REPLAY_EVENT_TO_EVENT_INDEXER command received");
        final UUID commandRuntimeId = jmxCommandRuntimeParameters.getCommandRuntimeId();
        final Optional<String> eventSourceName = ofNullable(jmxCommandRuntimeParameters.getCommandRuntimeString());
        try {
            replayEventToComponentRunner.run(commandId, commandRuntimeId, EVENT_INDEXER, eventSourceName);
            fireEvent(COMMAND_COMPLETE, command, commandId, "REPLAY_EVENT_TO_EVENT_INDEXER command completed");
        } catch (Exception e) {
            logger.error("REPLAY_EVENT_TO_EVENT_INDEXER failed. commandId {}, commandRuntimeId {}", commandId, commandRuntimeId, e);
            fireEvent(COMMAND_FAILED, command, commandId, "REPLAY_EVENT_TO_EVENT_INDEXER command failed");
        }
    }

    private void fireEvent(CommandState commandState, ReplayEventToEventIndexerCommand command, UUID commandId, String message) {
        stateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                commandId,
                command,
                commandState,
                clock.now(),
                message
        ));
    }
}
