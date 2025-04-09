package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.event.sourcing.subscription.manager.DefaultSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventBufferProcessor;
import uk.gov.justice.services.subscription.SubscriptionManager;

import javax.inject.Inject;

public class DefaultSubscriptionManagerFactory {

    @Inject
    private EventBufferProcessorFactory eventBufferProcessorFactory;

    public SubscriptionManager create(final String componentName) {

        final EventBufferProcessor eventBufferProcessor = eventBufferProcessorFactory.create(componentName);

        return new DefaultSubscriptionManager(eventBufferProcessor);
    }
}
