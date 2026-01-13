package uk.gov.justice.eventsourcing.discovery.workers;

import uk.gov.justice.eventsourcing.discovery.subscription.SourceComponentPair;
import uk.gov.justice.eventsourcing.discovery.subscription.SubscriptionSourceComponentFinder;

import java.util.List;

import javax.inject.Inject;

public class EventDiscoveryWorker {

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private EventDiscoverer eventDiscoverer;

    public void runEventDiscovery() {

        final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                .findSourceComponentPairsFromSubscriptionRegistry();

        sourceComponentPairs.forEach(eventDiscoverer::runEventDiscoveryForSourceComponentPair);
    }
}
