package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;

import javax.transaction.Transactional;

public class DefaultSubscriptionManager implements SubscriptionManager {

    private final EventBufferProcessor eventBufferProcessor;

    public DefaultSubscriptionManager(final EventBufferProcessor eventBufferProcessor) {
        this.eventBufferProcessor = eventBufferProcessor;
    }

    @Transactional(REQUIRED)
    @Override
    public void process(final JsonEnvelope incomingJsonEnvelope) {
        eventBufferProcessor.processWithEventBuffer(incomingJsonEnvelope);
    }
}
