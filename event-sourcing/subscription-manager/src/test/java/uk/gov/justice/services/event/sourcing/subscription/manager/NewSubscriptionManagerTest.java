package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class NewSubscriptionManagerTest {

    private NewEventBufferManager newEventBufferManager = mock(NewEventBufferManager.class);
    private SubscriptionEventProcessor subscriptionEventProcessor = mock(SubscriptionEventProcessor.class);
    private final String componentName = "some-component";

    private final NewSubscriptionManager newSubscriptionManager = new NewSubscriptionManager(
            newEventBufferManager,
            null,
            null,
            null,
            null,
            componentName
    );

    @SuppressWarnings("unchecked")
    @Test
    public void shouldAlwaysProcessIncomingEnvelopeIfNoBufferedEventsFoundInEventBuffer() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);

        when(newEventBufferManager.getNextFromEventBuffer(incomingJsonEnvelope, componentName)).thenReturn(empty());

        newSubscriptionManager.process(incomingJsonEnvelope);

        verify(subscriptionEventProcessor).processSingleEvent(incomingJsonEnvelope, componentName);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldProcessAllEventsFoundInEventBufferBeforeTheIncomingEnvelope() throws Exception {

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final JsonEnvelope jsonEnvelopeFromBuffer_1 = mock(JsonEnvelope.class);
        final JsonEnvelope jsonEnvelopeFromBuffer_2 = mock(JsonEnvelope.class);

        when(newEventBufferManager.getNextFromEventBuffer(incomingJsonEnvelope, componentName))
                .thenReturn(of(jsonEnvelopeFromBuffer_1), of(jsonEnvelopeFromBuffer_2), empty());

        newSubscriptionManager.process(incomingJsonEnvelope);

        final InOrder inOrder = inOrder(subscriptionEventProcessor);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(jsonEnvelopeFromBuffer_1, componentName);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(jsonEnvelopeFromBuffer_2, componentName);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(incomingJsonEnvelope, componentName);
    }
}