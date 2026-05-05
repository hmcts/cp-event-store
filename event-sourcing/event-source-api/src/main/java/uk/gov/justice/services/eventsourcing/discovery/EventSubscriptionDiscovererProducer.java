package uk.gov.justice.services.eventsourcing.discovery;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

public class EventSubscriptionDiscovererProducer {

    @Inject
    @TransactionalDiscoverer
    private EventSubscriptionDiscoverer transactionalEventSubscriptionDiscoverer;

    @Inject
    @RestDiscoverer
    private EventSubscriptionDiscoverer restEventSubscriptionDiscoverer;

    @Inject
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Produces
    public EventSubscriptionDiscoverer eventSubscriptionDiscoverer() {
        if (eventDiscoveryConfig.accessEventStoreViaRest()) {
            return restEventSubscriptionDiscoverer;
        }
        return transactionalEventSubscriptionDiscoverer;
    }
}
