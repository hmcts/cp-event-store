package uk.gov.justice.services.eventstore.management.catchup.process;

import static java.util.stream.Collectors.toList;

import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.services.eventstore.management.events.catchup.SubscriptionCatchupDetails;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.List;

import jakarta.inject.Inject;

public class SubscriptionCatchupProvider {

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Inject
    private PriorityComparatorProvider priorityComparatorProvider;

    @Inject
    private CanCatchupFilter canCatchupFilter;

    @Inject
    private SubscriptionCatchupDetailsMapper subscriptionCatchupDetailsMapper;

    public List<SubscriptionCatchupDetails> getBySubscription(final CatchupCommand catchupCommand) {

        return subscriptionsDescriptorsRegistry
                .getAll()
                .stream()
                .filter(subscriptionsDescriptor -> canCatchupFilter.canCatchup(subscriptionsDescriptor, catchupCommand))
                .sorted(priorityComparatorProvider.getSubscriptionDescriptorComparator())
                .flatMap(subscriptionCatchupDetailsMapper::toSubscriptionCatchupDetails)
                .collect(toList());
    }
}
