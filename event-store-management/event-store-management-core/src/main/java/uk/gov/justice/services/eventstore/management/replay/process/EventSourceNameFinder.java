package uk.gov.justice.services.eventstore.management.replay.process;

import static java.lang.String.format;

import uk.gov.justice.services.eventstore.management.catchup.process.PriorityComparatorProvider;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.stream.Stream;

import javax.inject.Inject;

public class EventSourceNameFinder {

    private final SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    private final PriorityComparatorProvider priorityComparatorProvider;

    @Inject
    public EventSourceNameFinder(SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry, PriorityComparatorProvider priorityComparatorProvider) {
        this.subscriptionsDescriptorsRegistry = subscriptionsDescriptorsRegistry;
        this.priorityComparatorProvider = priorityComparatorProvider;
    }

    public String getEventSourceNameOf(String componentName) {

        return subscriptionsDescriptorsRegistry
                .getAll()
                .stream()
                .filter(subscriptionsDescriptor -> subscriptionsDescriptor.getServiceComponent().contains(componentName))
                .sorted(priorityComparatorProvider.getSubscriptionDescriptorComparator())
                .flatMap(this::getSubscriptions)
                .findFirst()
                .map(Subscription::getEventSourceName)
                .orElseThrow(() -> new ReplayEventFailedException("No event source name found for event listener"));
    }

    public String ensureEventSourceNameExistsInRegistry(final String eventSourceName, final String componentName) {

        return subscriptionsDescriptorsRegistry
                .getAll()
                .stream()
                .filter(subscriptionsDescriptor -> subscriptionsDescriptor.getServiceComponent().contains(componentName))
                .sorted(priorityComparatorProvider.getSubscriptionDescriptorComparator())
                .flatMap(this::getSubscriptions)
                .filter(subscription -> subscription.getEventSourceName().equals(eventSourceName))
                .findFirst()
                .map(Subscription::getEventSourceName)
                .orElseThrow(() -> new ReplayEventFailedException(format("No event source named '%s' found in subscriptions-descriptor.yaml file(s) for component '%s'", eventSourceName, componentName)));
    }

    private Stream<Subscription> getSubscriptions(SubscriptionsDescriptor subscriptionsDescriptor) {
        return subscriptionsDescriptor.getSubscriptions()
                .stream()
                .sorted(priorityComparatorProvider.getSubscriptionComparator());
    }
}
