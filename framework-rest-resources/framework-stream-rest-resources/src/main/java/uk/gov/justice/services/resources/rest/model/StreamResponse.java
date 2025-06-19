package uk.gov.justice.services.resources.rest.model;

import java.util.UUID;

public record StreamResponse(UUID streamId, Long position, Long lastKnownPosition, String source, String component,
                             String updatedAt, boolean upToDate, UUID errorId, Long errorPosition) {
}
