package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManagerDelegate;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import javax.inject.Inject;

public class NewSubscriptionAwareEventProcessor {

    @Inject
    private EventConverter eventConverter;

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Inject
    private NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;

    public int processWithEventBuffer(final PublishedEvent publishedEvent, final String subscriptionName) {
        final String componentName = subscriptionsDescriptorsRegistry.findComponentNameBy(subscriptionName);
        final JsonEnvelope eventEnvelope = eventConverter.envelopeOf(publishedEvent);
        newSubscriptionManagerDelegate.process(eventEnvelope, componentName);
        return 1;
    }
}
