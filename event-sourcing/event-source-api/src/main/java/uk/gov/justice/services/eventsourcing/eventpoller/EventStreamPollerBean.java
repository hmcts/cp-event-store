package uk.gov.justice.services.eventsourcing.eventpoller;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;

import javax.ejb.Stateless;
import javax.inject.Inject;
import java.util.UUID;
import java.util.stream.Stream;


@Stateless
public class EventStreamPollerBean {

    @Inject
    private EventJdbcRepository eventRepository;

    @Inject
    private EventStreamPollerConfig eventStreamPollerConfig;

    /**
     *  Retrieves a stream of envelopes for the given stream within the specified position range,
     *  ordered by position in ascending order.
     *
     * @param streamId the id of the stream to retrieve
     * @param fromPosition (exclusive) the position to read the stream from
     * @param toPosition (inclusive) the position to read the stream to
     * @return the stream of envelopes. Never returns null.
     */
    public Stream<Event> pollStreamEvents(final UUID streamId, final long fromPosition, final long toPosition) {
        return eventRepository.findByStreamIdInPositionRangeOrderByPositionAsc(streamId, fromPosition, toPosition, eventStreamPollerConfig.getBatchSize());
    }
}
