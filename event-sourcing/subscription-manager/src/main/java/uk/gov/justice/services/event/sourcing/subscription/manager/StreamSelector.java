package uk.gov.justice.services.event.sourcing.subscription.manager;

import java.util.Optional;
import uk.gov.justice.services.event.buffer.core.repository.subscription.LockedStreamStatus;

public interface StreamSelector {

    Optional<LockedStreamStatus> findStreamToProcess(final String source, final String component);
}
