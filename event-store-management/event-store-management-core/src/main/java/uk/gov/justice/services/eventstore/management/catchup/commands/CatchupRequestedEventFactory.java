package uk.gov.justice.services.eventstore.management.catchup.commands;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.CatchupRequestedEvent;
import uk.gov.justice.services.jmx.api.parameters.JmxCommandRuntimeParameters;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.inject.Inject;

public class CatchupRequestedEventFactory {

    @Inject
    private UtcClock clock;

    public CatchupRequestedEvent create(
            final CatchupCommand catchupCommand,
            final UUID commandId,
            final JmxCommandRuntimeParameters jmxCommandRuntimeParameters) {

        final ZonedDateTime now = clock.now();

        final UUID runFromEventId = jmxCommandRuntimeParameters.getCommandRuntimeId();

        if (runFromEventId == null) {
            return new CatchupRequestedEvent(
                    commandId,
                    catchupCommand,
                    now);
        }

        return new CatchupRequestedEvent(
                commandId,
                catchupCommand,
                now,
                runFromEventId);
    }
}
