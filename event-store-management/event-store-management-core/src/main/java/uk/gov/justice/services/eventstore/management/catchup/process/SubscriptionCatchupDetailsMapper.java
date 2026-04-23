package uk.gov.justice.services.eventstore.management.catchup.process;

import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;

import java.util.stream.Stream;

import jakarta.inject.Inject;

public class SubscriptionCatchupDetailsMapper {

    @Inject
    private PriorityComparatorProvider priorityComparatorProvider;

    @Inject
    private EventSourceNameFilter eventSourceNameFilter;

    public Stream<SubscriptionCatchupDetails> toSubscriptionCatchupDetails(final SubscriptionsDescriptor subscriptionsDescriptor) {

        final String componentName = subscriptionsDescriptor.getServiceComponent();

        return subscriptionsDescriptor.getSubscriptions()
                .stream()
                .filter(subscription -> eventSourceNameFilter.shouldRunCatchupAgainstEventSource(subscription))
                .sorted(priorityComparatorProvider.getSubscriptionComparator())
                .map(subscription ->
                        new SubscriptionCatchupDetails(
                                subscription.getName(),
                                subscription.getEventSourceName(),
                                componentName
                        )
                );
    }
}
