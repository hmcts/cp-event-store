package uk.gov.justice.eventsourcing.discovery.dataaccess;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public record EventSubscriptionStatus(
    String source,
    String component,
    Optional<UUID> latestEventId,
    ZonedDateTime updatedAt) {
}
