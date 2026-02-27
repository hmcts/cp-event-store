package uk.gov.justice.services.resources.rest.model;

import java.util.UUID;

public record ResetStreamRetryCountResponse(
        boolean success,
        String message,
        UUID streamId,
        String source,
        String component) {
}
