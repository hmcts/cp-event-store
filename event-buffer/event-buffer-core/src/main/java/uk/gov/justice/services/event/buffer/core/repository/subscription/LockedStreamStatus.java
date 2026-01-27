package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.util.UUID;

public record LockedStreamStatus(
        UUID streamId,
        Long position,
        Long latestKnownPosition) {

}
