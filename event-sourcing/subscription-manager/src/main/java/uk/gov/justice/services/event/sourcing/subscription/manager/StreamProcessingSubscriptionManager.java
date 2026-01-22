package uk.gov.justice.services.event.sourcing.subscription.manager;

import javax.inject.Inject;
import javax.transaction.Transactional;

import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;

public class StreamProcessingSubscriptionManager {

    @Inject
    private StreamEventProcessor streamEventProcessor;

    @Transactional(NOT_SUPPORTED)
    public void process(final String source, final String component) {
        while (streamEventProcessor.processSingleEvent(source, component)) {
            // Continue processing events until no more events are available
        }
    }
}
