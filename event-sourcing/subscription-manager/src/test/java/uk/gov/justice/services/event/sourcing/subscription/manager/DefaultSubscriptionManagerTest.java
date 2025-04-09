package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;

public class DefaultSubscriptionManagerTest {

    private final EventBufferProcessor eventBufferProcessor = mock(EventBufferProcessor.class);
    private final DefaultSubscriptionManager defaultSubscriptionManager = new DefaultSubscriptionManager(eventBufferProcessor);

    @Test
    public void shouldProcessWithEventBuffer() {
        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);

        defaultSubscriptionManager.process(incomingJsonEnvelope);

        verify(eventBufferProcessor).processWithEventBuffer(incomingJsonEnvelope);
    }
}
