package uk.gov.justice.services.eventsourcing.eventpublishing;

import javax.inject.Inject;

public class EventLinkingWorker {

    @Inject
    private EventNumberLinker eventNumberLinker;

    public void linkNewEvents(final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator) {

        while (sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()) {
            final int linked = eventNumberLinker.findAndLinkEventsInBatch();
            if (linked == 0) {
                break;
            }
        }
    }
}