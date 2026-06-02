package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManagerDelegate;
import uk.gov.justice.services.subscription.SubscriptionManager;

import jakarta.inject.Inject;

public class NewSubscriptionManagerFactory {

    @Inject
    private NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;

    public SubscriptionManager create(final String componentName) {
        return new NewSubscriptionManager(newSubscriptionManagerDelegate, componentName);
    }
}
