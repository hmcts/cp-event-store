package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class OldestStreamSelector implements StreamSelector {

    @Inject
    NewStreamStatusRepository streamStatusRepository;

    @Inject
    StreamRetryConfiguration streamRetryConfiguration;

    @Override
    public Optional<LockedStreamStatus> findStreamToProcess(final String source, final String component) {
        return streamStatusRepository.findOldestStreamToProcessByAcquiringLock(source, component, streamRetryConfiguration.getMaxRetries());
    }
}
