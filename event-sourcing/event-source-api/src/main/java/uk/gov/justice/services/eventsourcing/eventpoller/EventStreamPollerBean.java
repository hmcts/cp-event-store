package uk.gov.justice.services.eventsourcing.eventpoller;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.ejb.Stateless;
import javax.inject.Inject;
import java.util.List;
import java.util.UUID;


@Stateless
public class EventStreamPollerBean {

    @Inject
    private LinkedEventSource linkedEventSource;

    @Inject
    private EventStreamPollerConfig eventStreamPollerConfig;

    @Inject
    private EventConverter eventConverter;

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
        return linkedEventSource.pollStreamEvents(streamId, fromPosition, toPosition, eventStreamPollerConfig.getBatchSize())
                .map(eventConverter::envelopeOf)
                .toList();
    }
}
