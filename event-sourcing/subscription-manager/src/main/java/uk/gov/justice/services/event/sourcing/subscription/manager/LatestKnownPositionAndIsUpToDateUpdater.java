package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamUpdateContext;

import java.util.UUID;

import jakarta.inject.Inject;

public class LatestKnownPositionAndIsUpToDateUpdater {

    @Inject
    private NewStreamStatusRepository newStreamStatusRepository;

    public void updateIfNecessary(
            final StreamUpdateContext streamUpdateContext,
            final UUID streamId,
            final String source,
            final String componentName) {

        if (streamUpdateContext.latestKnownStreamPosition() < streamUpdateContext.incomingEventPosition()) {
            newStreamStatusRepository.updateLatestKnownPositionAndIsUpToDateToFalse(
                    streamId,
                    source,
                    componentName,
                    streamUpdateContext.incomingEventPosition()
            );
        }
    }
}
