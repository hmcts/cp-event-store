package uk.gov.justice.services.eventsourcing.source.api.service.core;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingEventRange;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Source of published events.
 */
public interface LinkedEventSource {

    /**
     * returns a (Java) stream of all events since the provided event number. That is all events
     * who's eventNumber is greater than the provided event number
     *
     * @param eventNumber An event number to search from
     * @return a Java Stream of Events
     */
    Stream<LinkedEvent> findEventsSince(final long eventNumber);

    Stream<LinkedEvent> findEventRange(final MissingEventRange missingEventRange);

    Optional<LinkedEvent> findByEventId(final UUID id);

    Long getHighestPublishedEventNumber();
}
