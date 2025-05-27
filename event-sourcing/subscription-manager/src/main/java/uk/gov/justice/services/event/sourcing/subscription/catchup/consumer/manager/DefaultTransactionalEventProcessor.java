package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import uk.gov.justice.services.event.sourcing.subscription.manager.CatchupEventBufferProcessor;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;
import javax.transaction.Transactional;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

public class DefaultTransactionalEventProcessor {

    @Inject
    private EventConverter eventConverter;

    @Inject
    private CatchupEventBufferProcessor catchupEventBufferProcessor;

    @Transactional(REQUIRES_NEW)
    public int processWithEventBuffer(final PublishedEvent publishedEvent, final String subscriptionName) {
        final JsonEnvelope eventEnvelope = eventConverter.envelopeOf(publishedEvent);
        catchupEventBufferProcessor.processWithEventBuffer(eventEnvelope, subscriptionName);
        return 1;
    }
}
