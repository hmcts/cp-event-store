package uk.gov.justice.services.eventstore.management.catchup.state;

import static com.google.common.collect.ImmutableList.copyOf;

import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.inject.Singleton;

@Singleton
public class CatchupErrorStateManager {

    private final List<CatchupError> eventCatchupErrors = new CopyOnWriteArrayList<>();
    private final List<CatchupError> indexCatchupErrors = new CopyOnWriteArrayList<>();

    public void add(final CatchupError catchupError, final CatchupCommand catchupCommand) {
        listFor(catchupCommand).add(catchupError);
    }

    public List<CatchupError> getErrors(final CatchupCommand catchupCommand) {
        return copyOf(listFor(catchupCommand));
    }

    public void clear(final CatchupCommand catchupCommand) {
        listFor(catchupCommand).clear();
    }

    private List<CatchupError> listFor(final CatchupCommand catchupCommand) {
        if (catchupCommand.isEventCatchup()) {
            return eventCatchupErrors;
        }

        return indexCatchupErrors;
    }
}
