package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import java.util.Map;
import java.util.UUID;

public record EventLinkedEvent(Map<UUID, Long> streamPositions) {

}
