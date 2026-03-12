package uk.gov.justice.services.eventstore.management.commands;

import uk.gov.justice.services.jmx.api.command.BaseSystemCommand;

public class VerifyCatchupCommand extends BaseSystemCommand implements VerificationCommand {

    public static final String VERIFY_CATCHUP = "VERIFY_CATCHUP";
    private static final String DESCRIPTION = "Runs various verifications on event_log, processed_event and stream_buffer to check that CATCHUP has run successfully";

    public VerifyCatchupCommand() {
        super(VERIFY_CATCHUP, DESCRIPTION);
    }

    @Override
    public boolean isDisabledByPullMechanism() {
        return true;
    }
}
