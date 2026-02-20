package uk.gov.justice.services.eventsourcing.discovery;

import java.util.Optional;
import java.util.UUID;

public interface EventSubscriptionDiscoverer {

    DiscoveryResult discoverNewEvents(Optional<UUID> latestKnownEventId);
}
