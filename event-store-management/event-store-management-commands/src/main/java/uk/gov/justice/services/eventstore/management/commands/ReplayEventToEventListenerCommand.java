package uk.gov.justice.services.eventstore.management.commands;

import uk.gov.justice.services.jmx.api.command.BaseSystemCommand;

public class ReplayEventToEventListenerCommand extends BaseSystemCommand {

    public static final String REPLAY_EVENT_TO_EVENT_LISTENER = "REPLAY_EVENT_TO_EVENT_LISTENER";
    private static final String DESCRIPTION = "Replay single event to event listener";

    public ReplayEventToEventListenerCommand() {
        super(REPLAY_EVENT_TO_EVENT_LISTENER, DESCRIPTION);
    }

    @Override
    public boolean isDisabledByPullMechanism() {
        return true;
    }
}
