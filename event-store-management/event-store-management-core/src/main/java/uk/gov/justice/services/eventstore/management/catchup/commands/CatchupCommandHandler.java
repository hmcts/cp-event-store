package uk.gov.justice.services.eventstore.management.catchup.commands;

import static java.lang.String.format;
import static uk.gov.justice.services.eventstore.management.commands.EventCatchupCommand.CATCHUP;
import static uk.gov.justice.services.eventstore.management.commands.IndexerCatchupCommand.INDEXER_CATCHUP;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.commands.IndexerCatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupRequestedEvent;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;
import uk.gov.justice.services.jmx.command.HandlesSystemCommand;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;

public class CatchupCommandHandler {

    @Inject
    private Event<CatchupRequestedEvent> catchupRequestedEventFirer;

    @Inject
    private CatchupRequestedEventFactory catchupRequestedEventFactory;

    @Inject
    private Logger logger;

    @HandlesSystemCommand(CATCHUP)
    public void catchupEvents(
            final CatchupCommand catchupCommand,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {
        doCatchup(catchupCommand, commandId, jmxCommandRuntimeParameters);
    }

    @HandlesSystemCommand(INDEXER_CATCHUP)
    public void catchupSearchIndexes(
            final IndexerCatchupCommand indexerCatchupCommand,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {
        doCatchup(indexerCatchupCommand, commandId, jmxCommandRuntimeParameters);
    }

    private void doCatchup(
            final CatchupCommand catchupCommand,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {

        final CatchupRequestedEvent catchupRequestedEvent = catchupRequestedEventFactory.create(
                catchupCommand,
                commandId,
                jmxCommandRuntimeParameters
        );

        logger.info(format("Received command '%s' at %tr", catchupCommand, catchupRequestedEvent.getCatchupRequestedAt()));

        catchupRequestedEventFirer.fire(catchupRequestedEvent);
    }
}
