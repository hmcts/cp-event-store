package uk.gov.justice.services.eventstore.management.catchup.process;

import uk.gov.justice.services.eventstore.management.commands.CatchupCommand;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;

import jakarta.inject.Inject;

public class CanCatchupFilter {

    @Inject
    private CatchupTypeSelector catchupTypeSelector;

    public boolean canCatchup(final SubscriptionsDescriptor subscriptionsDescriptor, final CatchupCommand catchupCommand) {

        final String componentName = subscriptionsDescriptor.getServiceComponent();
        final boolean eventCatchup = catchupTypeSelector.isEventCatchup(componentName, catchupCommand);
        final boolean indexerCatchup = catchupTypeSelector.isIndexerCatchup(componentName, catchupCommand);

        return eventCatchup || indexerCatchup;
    }
}
