package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewEventBufferProcessorTest {


    @Mock
    private NewEventBufferManager newEventBufferManager;

    @Mock
    private SubscriptionEventProcessor subscriptionEventProcessor;

    @InjectMocks
    private NewEventBufferProcessor newEventBufferProcessor;

    @SuppressWarnings("unchecked")
    @Test
    public void shouldProcesAllEventsFromEventBufferIn() throws Exception {

        final String componentName = "some-component-name";
        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final JsonEnvelope nextJsonEnvelopeFromBuffer_1 = mock(JsonEnvelope.class);
        final JsonEnvelope nextJsonEnvelopeFromBuffer_2 = mock(JsonEnvelope.class);

        when(newEventBufferManager.getNextFromEventBuffer(incomingJsonEnvelope, componentName)).thenReturn(
                of(nextJsonEnvelopeFromBuffer_1),
                of(nextJsonEnvelopeFromBuffer_2),
                of(incomingJsonEnvelope),
                empty()
        );

        newEventBufferProcessor.process(incomingJsonEnvelope, componentName);

        final InOrder inOrder = inOrder(subscriptionEventProcessor);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(nextJsonEnvelopeFromBuffer_1, componentName);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(nextJsonEnvelopeFromBuffer_2, componentName);
        inOrder.verify(subscriptionEventProcessor).processSingleEvent(incomingJsonEnvelope, componentName);

        inOrder.verifyNoMoreInteractions();
    }

}