package uk.gov.justice.services.eventstore.management.commands;

import static uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand.CATCHUP;

import uk.gov.justice.services.jmx.api.command.SystemCommand;


public interface CatchupCommand extends SystemCommand {

    default boolean isEventCatchup() {
        return CATCHUP.equals(getName());
    }

    @Override
    default boolean isDisabledByPullMechanism() {
        return true;
    }
}
