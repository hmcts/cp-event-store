package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.sourcing.subscription.manager.timer.StreamProcessingConfig;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class OldestStreamSelector implements StreamSelector {

    @Inject
    NewStreamStatusRepository streamStatusRepository;

    @Inject
    StreamProcessingConfig streamProcessingConfig;

    @Override
    public Optional<LockedStreamStatus> findStreamToProcess(final String source, final String component) {
        return streamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, component, streamProcessingConfig.getMaxRetries());
    }
}
