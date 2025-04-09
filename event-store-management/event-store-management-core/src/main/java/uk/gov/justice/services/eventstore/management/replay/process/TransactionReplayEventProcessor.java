package uk.gov.justice.services.eventstore.management.replay.process;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.event.sourcing.subscription.manager.EventBufferProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.EventBufferProcessorFactory;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class TransactionReplayEventProcessor {

    @Inject
    private EventBufferProcessorFactory eventBufferProcessorFactory;


    @Transactional(REQUIRES_NEW)
    public void processWithEventBuffer(final String componentName, final JsonEnvelope eventEnvelope) {
        final EventBufferProcessor eventBufferProcessor = eventBufferProcessorFactory.create(componentName);
        eventBufferProcessor.processWithEventBuffer(eventEnvelope);
    }
}
