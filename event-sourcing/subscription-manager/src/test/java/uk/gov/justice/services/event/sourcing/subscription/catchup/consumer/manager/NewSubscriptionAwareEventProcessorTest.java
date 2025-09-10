package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManagerDelegate;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NewSubscriptionAwareEventProcessorTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Mock
    private NewSubscriptionManagerDelegate newSubscriptionManagerDelegate;

    @Mock
    private EventConverter eventConverter;

    @InjectMocks
    private NewSubscriptionAwareEventProcessor newSubscriptionAwareEventProcessor;

    @Test
    void processWithEventBuffer() {
        final String subscriptionName = "subscriptionName";
        String compName = "compName";
        PublishedEvent publishedEvent = mock(PublishedEvent.class);
        JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(subscriptionsDescriptorsRegistry.findComponentNameBy(subscriptionName)).thenReturn(compName);
        when(eventConverter.envelopeOf(publishedEvent)).thenReturn(jsonEnvelope);

        // run
        int result = newSubscriptionAwareEventProcessor.processWithEventBuffer(publishedEvent, subscriptionName);

        // verify
        assertEquals(1, result);
        verify(newSubscriptionManagerDelegate).process(eq(jsonEnvelope), eq(compName));
        verifyNoMoreInteractions(subscriptionsDescriptorsRegistry, newSubscriptionManagerDelegate, eventConverter);
    }
}