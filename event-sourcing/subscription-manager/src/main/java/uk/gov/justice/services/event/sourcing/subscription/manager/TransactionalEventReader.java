package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.api.service.core.LinkedEventSource;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class TransactionalEventReader {

    @Inject
    private LinkedEventSourceProvider linkedEventSourceProvider;

    @Inject
    private EventConverter eventConverter;

    @Transactional(REQUIRES_NEW)
    public Optional<JsonEnvelope> readNextEvent(final String source, final UUID streamId, final Long position) {
        final LinkedEventSource linkedEventSource = linkedEventSourceProvider.getLinkedEventSource(source);
        final Optional<LinkedEvent> linkedEvent = linkedEventSource.findNextEventInTheStreamAfterPosition(streamId, position);
        return linkedEvent.map(eventConverter::envelopeOf);
    }
}