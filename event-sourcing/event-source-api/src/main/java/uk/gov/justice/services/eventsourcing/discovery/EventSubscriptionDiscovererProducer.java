package uk.gov.justice.services.eventsourcing.discovery;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

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
