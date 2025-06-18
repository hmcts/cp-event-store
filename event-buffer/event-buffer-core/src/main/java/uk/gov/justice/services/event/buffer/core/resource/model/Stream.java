package uk.gov.justice.services.event.buffer.core.resource.model;

import java.util.Optional;
import java.util.UUID;

public record Stream(UUID streamId, Long position, Long lastKnownPosition, String source, String component,
                     String updatedAt, boolean upToDate, Optional<UUID> errorId, Optional<Long> errorPosition) {

}
