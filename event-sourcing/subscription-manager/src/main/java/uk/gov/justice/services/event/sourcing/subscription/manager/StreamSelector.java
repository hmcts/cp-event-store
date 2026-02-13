package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;

import java.util.Optional;

public interface StreamSelector {

    Optional<LockedStreamStatus> findStreamToProcess(final String source, final String component);
}
