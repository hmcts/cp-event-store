package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;

public class NewSubscriptionManagerTest {

    private final NewSubscriptionManagerDelegate newSubscriptionManagerDelegate = mock(NewSubscriptionManagerDelegate.class);
    private final EventPullConfiguration eventPullConfiguration = mock(EventPullConfiguration.class);
    private final String componentName = "SOME-COMPONENT-NAME";

    private final NewSubscriptionManager newSubscriptionManager = new NewSubscriptionManager(
            newSubscriptionManagerDelegate,
            eventPullConfiguration,
            componentName
    );

    @Test
    public void shouldPassThroughToTheNewSubscriptionManagerDelegateWithTheComponentNameWhenPullMechanismFlagIsTrue() throws Exception {
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);

        newSubscriptionManager.process(incomingJsonEnvelope);

        verify(newSubscriptionManagerDelegate).process(incomingJsonEnvelope, componentName);
    }

    @Test
    public void shouldPassThroughToTheNewSubscriptionManagerDelegateWithTheComponentNameWhenPullMechanismFlagIsFalse() throws Exception {
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(false);
        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);

        newSubscriptionManager.process(incomingJsonEnvelope);

        verify(newSubscriptionManagerDelegate, never()).process(incomingJsonEnvelope, componentName);
    }
}