package uk.gov.justice.services.eventstore.management.commands;

import uk.gov.justice.services.jmx.api.command.BaseSystemCommand;

public class ReplayEventToEventIndexerCommand extends BaseSystemCommand {

    public static final String REPLAY_EVENT_TO_EVENT_INDEXER = "REPLAY_EVENT_TO_EVENT_INDEXER";
    private static final String DESCRIPTION = "Replay single event to event indexer";

    public ReplayEventToEventIndexerCommand() {
        super(REPLAY_EVENT_TO_EVENT_INDEXER, DESCRIPTION);
    }

    @Override
    public boolean isDisabledByPullMechanism() {
        return true;
    }
}
