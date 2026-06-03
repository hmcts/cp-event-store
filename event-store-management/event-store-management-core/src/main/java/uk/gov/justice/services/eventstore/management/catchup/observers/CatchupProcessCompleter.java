package uk.gov.justice.services.eventstore.management.catchup.observers;

import uk.gov.justice.services.eventstore.management.catchup.state.CatchupError;
import uk.gov.justice.services.eventstore.management.catchup.state.CatchupErrorStateManager;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.util.List;
import java.util.UUID;

import jakarta.inject.Inject;

public class CatchupProcessCompleter {

    @Inject
    private CatchupErrorStateManager catchupErrorStateManager;

    @Inject
    private CatchupCompletionEventFirer catchupCompletionEventFirer;

    public void handleCatchupComplete(final UUID commandId, final CatchupCommand catchupCommand) {

        final List<CatchupError> errors = catchupErrorStateManager.getErrors(catchupCommand);

        if (errors.isEmpty()) {
            catchupCompletionEventFirer.completeSuccessfully(commandId, catchupCommand);
        } else {
            catchupCompletionEventFirer.failCatchup(commandId, catchupCommand, errors);
        }
    }
}
