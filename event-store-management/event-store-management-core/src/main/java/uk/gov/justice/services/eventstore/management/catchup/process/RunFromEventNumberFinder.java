package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

public class RunFromEventNumberFinder {

    protected static final long DEFAULT_FIRST_EVENT_NUMBER = 1L;

    @Inject
    private LinkedEventSource linkedEventSource;

    @Inject
    private Logger logger;

    public Long findEventNumberToRunFrom(final Optional<UUID> runFromEventId, final String source) {

        if (runFromEventId.isPresent()) {
            final UUID eventId = runFromEventId.get();
            final Optional<LinkedEvent> linkedEvent = linkedEventSource.findByEventId(eventId);

            if (linkedEvent.isPresent()) {
                    return linkedEvent.get().getEventNumber().orElseThrow(
                            () -> new MissingEventNumberException(format("Event with id '%s' found in '%s' event_log table has null event number", eventId, source))
                    );
            } else {
                logger.info(format(
                        "No event with id '%s' found in '%s' event_log table. Running from eventNumber '%d'",
                        eventId,
                        source,
                        DEFAULT_FIRST_EVENT_NUMBER));
            }
        }

        return DEFAULT_FIRST_EVENT_NUMBER;
    }
}
