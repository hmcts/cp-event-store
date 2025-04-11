package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.event.sourcing.subscription.manager.NewEventBufferManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.SubscriptionEventProcessor;
import uk.gov.justice.services.subscription.SubscriptionManager;

import javax.inject.Inject;

public class NewSubscriptionManagerFactory {

    @Inject
    private NewEventBufferManager newEventBufferManager;

    @Inject
    private SubscriptionEventProcessorFactory subscriptionEventProcessorFactory;

    public SubscriptionManager create(final String componentName) {

        final SubscriptionEventProcessor subscriptionEventProcessor = subscriptionEventProcessorFactory.create(componentName);

        return new NewSubscriptionManager(
                newEventBufferManager,
                null,
                null,
                null,
                null,
                componentName);
    }
}
