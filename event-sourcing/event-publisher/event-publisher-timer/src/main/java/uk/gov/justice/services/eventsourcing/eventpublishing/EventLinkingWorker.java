package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

import javax.inject.Inject;

public class EventLinkingWorker {

    @Inject
    private EventNumberLinker eventNumberLinker;

    public void linkNewEvents(final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator) {

        boolean continueRunning = true;
        while (continueRunning) {
            continueRunning = sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()
                              && eventNumberLinker.findAndAndLinkNextUnlinkedEvent();
        }
    }
}
