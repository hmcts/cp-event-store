package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventJdbcRepository;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class TransactionalNextEventReader implements NextEventReader {

    @Inject
    private EventJdbcRepository eventJdbcRepository;

    @Inject
    private EventConverter eventConverter;

    @Transactional(REQUIRES_NEW)
    @Override
    public Optional<JsonEnvelope> read(final UUID streamId, final Long position) {
        return eventJdbcRepository.findNextEventInTheStreamAfterPosition(streamId, position)
                .map(eventConverter::envelopeOf);
    }
}
