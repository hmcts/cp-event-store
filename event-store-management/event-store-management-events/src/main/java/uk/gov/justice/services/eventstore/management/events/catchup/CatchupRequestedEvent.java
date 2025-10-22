package uk.gov.justice.services.eventstore.management.events.catchup;

import static java.util.Optional.ofNullable;

import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class CatchupRequestedEvent {

    private final UUID commandId;
    private final CatchupCommand catchupCommand;
    private final ZonedDateTime catchupRequestedAt;
    private final UUID runFromEventId;

    public CatchupRequestedEvent(
            final UUID commandId,
            final CatchupCommand catchupCommand,
            final ZonedDateTime catchupRequestedAt) {
        this(commandId, catchupCommand, catchupRequestedAt, null);
    }

    public CatchupRequestedEvent(
            final UUID commandId,
            final CatchupCommand catchupCommand,
            final ZonedDateTime catchupRequestedAt,
            final UUID runFromEventId) {
        this.commandId = commandId;
        this.catchupCommand = catchupCommand;
        this.catchupRequestedAt = catchupRequestedAt;
        this.runFromEventId = runFromEventId;
    }

    public UUID getCommandId() {
        return commandId;
    }

    public CatchupCommand getCatchupCommand() {
        return catchupCommand;
    }

    public ZonedDateTime getCatchupRequestedAt() {
        return catchupRequestedAt;
    }

    public Optional<UUID> getRunFromEventId() {
        return ofNullable(runFromEventId);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final CatchupRequestedEvent that = (CatchupRequestedEvent) o;
        return Objects.equals(commandId, that.commandId) && Objects.equals(catchupCommand, that.catchupCommand) && Objects.equals(catchupRequestedAt, that.catchupRequestedAt) && Objects.equals(runFromEventId, that.runFromEventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandId, catchupCommand, catchupRequestedAt, runFromEventId);
    }

    @Override
    public String toString() {
        return "CatchupRequestedEvent{" +
               "commandId=" + commandId +
               ", catchupCommand=" + catchupCommand +
               ", catchupRequestedAt=" + catchupRequestedAt +
               ", runFromEventId=" + runFromEventId +
               '}';
    }
}
