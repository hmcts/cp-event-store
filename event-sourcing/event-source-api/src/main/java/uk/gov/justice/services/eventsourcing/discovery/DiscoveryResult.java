package uk.gov.justice.services.eventsourcing.discovery;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.StreamPosition;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public record DiscoveryResult(List<StreamPosition> streamPositions, Optional<UUID> latestKnownEventId) {
}
