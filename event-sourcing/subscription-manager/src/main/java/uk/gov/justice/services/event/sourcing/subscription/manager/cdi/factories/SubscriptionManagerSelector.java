package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import javax.inject.Inject;
import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.subscription.SubscriptionManager;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import static uk.gov.justice.services.core.annotation.Component.EVENT_INDEXER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

public class SubscriptionManagerSelector {

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionDescriptorRegistry;

    @Inject
    private DefaultSubscriptionManagerFactory defaultSubscriptionManagerFactory;

    @Inject
    private BackwardsCompatibleSubscriptionManagerFactory backwardsCompatibleSubscriptionManagerFactory;

    @Inject
    private EventErrorHandlingConfiguration eventErrorHandlingConfiguration;

    @Inject
    private NewSubscriptionManagerFactory newSubscriptionManagerFactory;

    public SubscriptionManager selectFor(final Subscription subscription) {

        final String subscriptionName = subscription.getName();
        final String componentName = subscriptionDescriptorRegistry.findComponentNameBy(subscriptionName);

        return selectFor(componentName);
    }

    public SubscriptionManager selectFor(final String componentName) {
        if(componentName.contains(EVENT_LISTENER) || componentName.contains(EVENT_INDEXER)) {

            if(eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()) {
                return newSubscriptionManagerFactory.create(componentName);
            }

            return defaultSubscriptionManagerFactory.create(componentName);
        }

        return backwardsCompatibleSubscriptionManagerFactory.create(componentName);
    }
}
