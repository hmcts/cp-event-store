package uk.gov.justice.services.event.sourcing.subscription.manager.cdi;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.subscription.domain.builders.SubscriptionBuilder.subscription;

import uk.gov.justice.services.cdi.QualifierAnnotationExtractor;
import uk.gov.justice.services.event.sourcing.subscription.manager.DefaultSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.subscription.annotation.SubscriptionName;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import jakarta.enterprise.inject.spi.InjectionPoint;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionManagerProducerTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionDescriptorRegistry;

    @Mock
    private QualifierAnnotationExtractor qualifierAnnotationExtractor;

    @Mock
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @InjectMocks
    private SubscriptionManagerProducer subscriptionManagerProducer;

    @Test
    public void shouldCreateSubscriptionManagersOnStartUp() {

        final InjectionPoint injectionPoint = mock(InjectionPoint.class);

        final SubscriptionName subscriptionName = mock(SubscriptionName.class);

        final String subscriptionNameString = "subscriptionName";
        final Subscription subscription = subscription()
                .withEventSourceName("eventSourceName")
                .withName(subscriptionNameString)
                .build();

        final DefaultSubscriptionManager defaultSubscriptionManager = mock(DefaultSubscriptionManager.class);

        when(qualifierAnnotationExtractor.getFrom(injectionPoint, SubscriptionName.class)).thenReturn(subscriptionName);
        when(subscriptionName.value()).thenReturn(subscriptionNameString);
        when(subscriptionDescriptorRegistry.getSubscriptionFor(subscriptionNameString)).thenReturn(subscription);
        when(subscriptionManagerSelector.selectFor(subscription)).thenReturn(defaultSubscriptionManager);

        assertThat(subscriptionManagerProducer.subscriptionManager(injectionPoint), is(defaultSubscriptionManager));
    }
}
