package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

import javax.inject.Inject;

public class EventPublishingWorker {

    @Inject
    private LinkedEventPublisher linkedEventPublisher;

    public void publishNewEvents(final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator) {

        boolean continueRunning = true;
        while (continueRunning) {
            continueRunning = sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()
                              && linkedEventPublisher.publishNextNewEvent();
        }
    }
}
