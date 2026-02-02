package uk.gov.justice.eventsourcing.discovery.workers;

import java.util.List;
import javax.inject.Inject;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

public class EventDiscoveryWorker {

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private EventDiscoverer eventDiscoverer;

    public void runEventDiscovery() {

        final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                .findListenerOrIndexerPairs();

        sourceComponentPairs.forEach(eventDiscoverer::runEventDiscoveryForSourceComponentPair);
    }
}
