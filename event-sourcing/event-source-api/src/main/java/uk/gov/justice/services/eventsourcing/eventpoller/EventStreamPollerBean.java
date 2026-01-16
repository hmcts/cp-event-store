package uk.gov.justice.services.eventsourcing.eventpoller;

import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcBasedEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.ejb.Stateless;
import javax.inject.Inject;
import java.util.List;
import java.util.UUID;


@Stateless
public class EventStreamPollerBean {

    @Inject
    private JdbcBasedEventRepository jdbcBasedEventRepository;

    @Inject
    private EventStreamPollerConfig eventStreamPollerConfig;

    /**
     *  Retrieves a list of envelopes for the given stream within the specified position range,
     *  ordered by position in ascending order.
     *
     * @param streamId the id of the stream to retrieve
     * @param fromPosition (exclusive) the position to read the stream from
     * @param toPosition (inclusive) the position to read the stream to
     * @return the list of envelopes. Never returns null.
     */
    public List<JsonEnvelope> pollStreamEvents(final UUID streamId, final long fromPosition, final long toPosition) {
        return jdbcBasedEventRepository.pollStreamEvents(streamId, fromPosition, toPosition, eventStreamPollerConfig.getBatchSize()).toList();
    }
}
