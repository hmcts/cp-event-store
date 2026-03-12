package uk.gov.justice.services.eventstore.management.commands;

import uk.gov.justice.services.jmx.api.command.BaseSystemCommand;

public class IndexerCatchupCommand extends BaseSystemCommand implements CatchupCommand {

    public static final String INDEXER_CATCHUP = "INDEXER_CATCHUP";
    private static final String DESCRIPTION = "Rebuilds the application search indexes";

    public IndexerCatchupCommand() {
        super(INDEXER_CATCHUP, DESCRIPTION);
    }

    @Override
    public boolean isDisabledByPullMechanism() {
        return true;
    }
}
