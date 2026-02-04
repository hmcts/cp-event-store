package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;


public class NewSubscriptionManager implements SubscriptionManager  {

    private final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;
    private final EventPullConfiguration eventPullConfiguration;
    private final String componentName;

    public NewSubscriptionManager(final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate, final EventPullConfiguration eventPullConfiguration, final String componentName) {
        this.newSubscriptionManagerDelegate = newSubscriptionManagerDelegate;
        this.eventPullConfiguration = eventPullConfiguration;
        this.componentName = componentName;
    }

    @Override
    public void process(final JsonEnvelope incomingJsonEnvelope) {
        if (eventPullConfiguration.shouldProcessEventsByPullMechanism()) {
            newSubscriptionManagerDelegate.process(incomingJsonEnvelope, componentName);
        }
    }
}
