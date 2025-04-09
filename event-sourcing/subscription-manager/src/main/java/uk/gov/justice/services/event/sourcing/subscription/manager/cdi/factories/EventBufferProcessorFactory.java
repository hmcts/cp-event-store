package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventBufferProcessor;

import javax.inject.Inject;

public class EventBufferProcessorFactory {

    @Inject
    private EventBufferService eventBufferService;

    @Inject
    private SubscriptionEventProcessorFactory subscriptionEventProcessorFactory;

    public EventBufferProcessor create(final String componentName) {

        final SubscriptionEventProcessor subscriptionEventProcessor = subscriptionEventProcessorFactory.create(componentName);

        return new EventBufferProcessor(
                eventBufferService,
                subscriptionEventProcessor,
                componentName);
    }
}
