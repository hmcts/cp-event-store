package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamPositions;

import java.util.UUID;

import javax.inject.Inject;

public class LatestKnownPositionAndIsUpToDateUpdater {

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    public void updateIfNecessary(
            final StreamPositions streamPositions,
            final UUID streamId,
            final String source,
            final String componentName) {

        if (streamPositions.latestKnownStreamPosition() < streamPositions.incomingEventPosition()) {
            newStreamStatusRepository.updateLatestKnownPositionAndIsUpToDateToFalse(
                    streamId,
                    source,
                    componentName,
                    streamPositions.incomingEventPosition()
            );
        }
    }
}
