package uk.gov.justice.eventsourcing.discovery.dataaccess;

import java.time.ZonedDateTime;
import java.util.UUID;

public record EventSubscriptionStatus(
    String source,
    String component,
    UUID latestEventId,
    Long latestKnownPosition,
    ZonedDateTime updatedAt) {
}
