package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

import jakarta.inject.Inject;

public class EventLinkingWorker {

    @Inject
    private EventNumberLinker eventNumberLinker;

    public void linkNewEvents(final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator) {

        while (sufficientTimeRemainingCalculator.hasSufficientProcessingTimeRemaining()) {
            if (eventNumberLinker.findAndLinkEventsInBatch().isEmpty()) {
                break;
            }
        }
    }
}