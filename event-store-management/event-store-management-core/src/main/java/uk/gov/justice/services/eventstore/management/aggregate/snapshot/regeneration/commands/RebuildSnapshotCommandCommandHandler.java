package uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands;

import static java.lang.String.format;
import static uk.gov.justice.services.eventstore.management.aggregate.snapshot.regeneration.commands.RebuildSnapshotCommand.REBUILD_SNAPSHOTS;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_COMPLETE;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_FAILED;
import static uk.gov.justice.services.jmx.api.domain.CommandState.COMMAND_IN_PROGRESS;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jmx.api.domain.CommandState;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.command.HandlesSystemCommand;
import uk.gov.justice.services.jmx.state.events.SystemCommandStateChangedEvent;

import java.util.UUID;

import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.slf4j.Logger;

public class RebuildSnapshotCommandCommandHandler {

    @Inject
    private Event<SystemCommandStateChangedEvent> stateChangedEventFirer;

    @Inject
    private RegenerateAggregateSnapshotBean regenerateAggregateSnapshotBean;

    @Inject
    private UtcClock clock;

    @Inject
    private Logger logger;

    @HandlesSystemCommand(REBUILD_SNAPSHOTS)
    public void regenerateAggregateSnapshot(
            final RebuildSnapshotCommand command,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {

        final UUID streamId = jmxCommandRuntimeParameters.getCommandRuntimeId();
        final String aggregateClassName = jmxCommandRuntimeParameters.getCommandRuntimeString();

        fireEvent(COMMAND_IN_PROGRESS, command, commandId, format("%s command received for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName));
        logger.info(format("%s command received for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName));

        try {
            regenerateAggregateSnapshotBean.runAggregateSnapshotRegeneration(streamId, aggregateClassName);
            fireEvent(COMMAND_COMPLETE, command, commandId, format("%s command completed for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName));
            logger.info(format("%s command complete for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName));
        } catch (final Exception e) {
            fireEvent(COMMAND_FAILED, command, commandId, format("%s command failed for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName));
            logger.error(format("%s failed for streamId '%s' and Aggregate class '%s'", command.getName(), streamId, aggregateClassName), e);
        }
    }

    private void fireEvent(CommandState commandState, RebuildSnapshotCommand command, UUID commandId, String message) {
        stateChangedEventFirer.fire(new SystemCommandStateChangedEvent(
                commandId,
                command,
                commandState,
                clock.now(),
                message
        ));
    }
}
