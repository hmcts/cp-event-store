package uk.gov.justice.subscription;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import uk.gov.justice.services.common.util.LazyValue;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

@Singleton
public class SubscriptionSourceComponentFinder {

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    private final LazyValue lazyValue = new LazyValue();

    public List<SourceComponentPair> findSourceComponentPairsFromSubscriptionRegistry() {

        return lazyValue.createIfAbsent(this::lookupAllInSubscriptionRegistry);
    }

    private List<SourceComponentPair> lookupAllInSubscriptionRegistry() {

        final List<SourceComponentPair> sourceComponentPairs = new ArrayList<>();

        final List<SubscriptionsDescriptor> subscriptionsDescriptors = subscriptionsDescriptorsRegistry.getAll();
        for(final SubscriptionsDescriptor subscriptionsDescriptor: subscriptionsDescriptors) {
            final List<Subscription> subscriptions = subscriptionsDescriptor.getSubscriptions();
            final String component = subscriptionsDescriptor.getServiceComponent();

            for(final Subscription subscription: subscriptions) {
                final String source = subscription.getEventSourceName();
                final SourceComponentPair sourceComponentPair = new SourceComponentPair(source, component);
                sourceComponentPairs.add(sourceComponentPair);
            }
        }

        return sourceComponentPairs;
    }
}
