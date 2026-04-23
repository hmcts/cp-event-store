package uk.gov.justice.eventsourcing.discovery;

import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoverer;
import uk.gov.justice.services.eventsourcing.discovery.RestDiscoverer;
import uk.gov.justice.subscription.registry.EventSourceDefinitionRegistry;

import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@RestDiscoverer
@ApplicationScoped
public class RestEventSubscriptionDiscoverer implements EventSubscriptionDiscoverer {

    @Inject
    private EventSourceDefinitionRegistry eventSourceDefinitionRegistry;

    @Inject
    private EventDiscoveryHttpClient eventDiscoveryHttpClient;

    @Override
    public DiscoveryResult discoverNewEvents(final Optional<UUID> latestKnownEventId, final int batchSize, final String source) {

        final String restUri = eventSourceDefinitionRegistry.getRestUri(source);
        return eventDiscoveryHttpClient.discoverEvents(restUri, latestKnownEventId, batchSize);
    }
}
