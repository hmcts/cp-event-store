package uk.gov.justice.services.event.buffer.core.repository.subscription;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public record StreamStatus(
        UUID streamId,
        Long position,
        String source,
        String component,
        Optional<UUID> streamErrorId,
        Optional<Long> streamErrorPosition,
        ZonedDateTime updatedAt,
        Long latestKnownPosition,
        Boolean isUpToDate) {
}
