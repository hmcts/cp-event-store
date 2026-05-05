package uk.gov.justice.services.eventsourcing.eventreader;

import static jakarta.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.eventsourcing.source.api.service.core.NextEventReader;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@TransactionalReader
public class TransactionalNextEventReader implements NextEventReader {

    @Inject
    private EventJdbcRepository eventJdbcRepository;

    @Inject
    private EventConverter eventConverter;

    @Transactional(REQUIRES_NEW)
    @Override
    public Optional<JsonEnvelope> read(final UUID streamId, final Long position, final String source) {
        return eventJdbcRepository.findNextEventInTheStreamAfterPosition(streamId, position)
                .map(eventConverter::envelopeOf);
    }
}
