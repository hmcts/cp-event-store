package uk.gov.justice.services.event.sourcing.subscription.manager;

import javax.inject.Inject;
import javax.transaction.Transactional;

import static javax.transaction.Transactional.TxType.NOT_SUPPORTED;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_FOUND;

import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

public class StreamProcessingSubscriptionManager {

    @Inject
    private StreamEventProcessor streamEventProcessor;

    @Transactional(NOT_SUPPORTED)
    public void process(final String source, final String component, SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator) {
        while (streamEventProcessor.processSingleEvent(source, component) == EVENT_FOUND && sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()) {
            // Continue processing events until no more events are available or timer expires
        }
    }
}
