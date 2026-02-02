package uk.gov.justice.services.event.sourcing.subscription.manager;

import javax.inject.Inject;
import javax.transaction.Transactional;

import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;

import java.util.function.Supplier;

public class StreamProcessingSubscriptionManager {

    @Inject
    private StreamEventProcessor streamEventProcessor;

    @Transactional(NOT_SUPPORTED)
    public void process(final String source, final String component, final Supplier<Boolean> testExpiration) {
        while (streamEventProcessor.processSingleEvent(source, component) && testExpiration.get()) {
            // Continue processing events until no more events are available or timer expires
        }
    }
}
