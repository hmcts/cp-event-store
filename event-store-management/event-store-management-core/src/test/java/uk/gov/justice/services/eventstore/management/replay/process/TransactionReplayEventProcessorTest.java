package uk.gov.justice.services.eventstore.management.replay.process;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories.SubscriptionManagerSelector;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.SubscriptionManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TransactionReplayEventProcessorTest {

    @Mock
    private SubscriptionManagerSelector subscriptionManagerSelector;

    @InjectMocks
    private TransactionReplayEventProcessor transactionReplayEventProcessor;

    @Test
    void shouldCreateEventBufferProcessorAndInvokeProcess() {
        final String componentName = "componentName";
        final JsonEnvelope eventEnvelope = mock(JsonEnvelope.class);
        final SubscriptionManager subscriptionManager = mock(SubscriptionManager.class);
        when(subscriptionManagerSelector.selectFor(componentName)).thenReturn(subscriptionManager);

        transactionReplayEventProcessor.process(componentName, eventEnvelope);

        verify(subscriptionManager).process(eventEnvelope);
    }
}