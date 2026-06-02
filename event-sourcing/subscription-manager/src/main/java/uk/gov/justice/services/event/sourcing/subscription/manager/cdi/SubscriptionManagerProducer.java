package uk.gov.justice.services.event.sourcing.subscription.manager.cdi;

import uk.gov.justice.services.cdi.QualifierAnnotationExtractor;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.subscription.SubscriptionManager;
import uk.gov.justice.services.subscription.annotation.SubscriptionName;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

@ApplicationScoped
public class SubscriptionManagerProducer {

    private final Map<String, SubscriptionManager> subscriptionManagerMap = new ConcurrentHashMap<>();

    @Inject
    SubscriptionManagerSelector subscriptionManagerSelector;

    @Inject
    @Any
    Instance<EventSource> eventSourceInstance;

    @Inject
    SubscriptionsDescriptorsRegistry subscriptionDescriptorRegistry;

    @Inject
    QualifierAnnotationExtractor qualifierAnnotationExtractor;

    @Produces
    @SubscriptionName
    public SubscriptionManager subscriptionManager(final InjectionPoint injectionPoint) {
        final SubscriptionName subscriptionName = qualifierAnnotationExtractor.getFrom(injectionPoint, SubscriptionName.class);

        final Subscription subscription = subscriptionDescriptorRegistry.getSubscriptionFor(subscriptionName.value());

        return subscriptionManagerMap.computeIfAbsent(subscription.getName(), k -> subscriptionManagerSelector.selectFor(subscription));
    }
}



