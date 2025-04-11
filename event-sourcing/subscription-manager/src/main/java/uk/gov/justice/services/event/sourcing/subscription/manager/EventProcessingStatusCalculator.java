package uk.gov.justice.services.event.sourcing.subscription.manager;

import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.EVENT_IS_OBSOLETE;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.SHOULD_PROCESS_EVENT;
import static uk.gov.justice.services.event.sourcing.subscription.manager.EventProcessingStatus.SHOULD_BUFFER_EVENT;

import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;

public class EventProcessingStatusCalculator {

    public EventProcessingStatus calculateProcessingStatus(final StreamPositions streamPositions) {

        final long incomingEventPosition = streamPositions.incomingEventPosition();
        final long currentStreamPosition = streamPositions.currentStreamPosition();

        if(incomingEventPosition <= currentStreamPosition) {
            return EVENT_IS_OBSOLETE;
        }

        if (incomingEventPosition - currentStreamPosition != 1) {
            return SHOULD_BUFFER_EVENT;
        }

        return SHOULD_PROCESS_EVENT;
    }


}
