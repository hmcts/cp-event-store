package uk.gov.justice.services.eventsourcing.discovery;

import java.util.Optional;
import java.util.UUID;

@RestDiscoverer
public class RestEventSubscriptionDiscoverer implements EventSubscriptionDiscoverer {

    @Override
    public DiscoveryResult discoverNewEvents(final Optional<UUID> latestKnownEventId, final int batchSize) {
        throw new UnsupportedOperationException("RestEventSubscriptionDiscoverer is not yet implemented");
    }
}
