package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewEventBufferManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.NewSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.SubscriptionEventProcessor;
import uk.gov.justice.services.subscription.SubscriptionManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewSubscriptionManagerFactoryTest {

    @Mock
    private EventBufferService eventBufferService;

    @Mock
    private NewEventBufferManager newEventBufferManager;

    @Mock
    private SubscriptionEventProcessorFactory subscriptionEventProcessorFactory;

    @InjectMocks
    private NewSubscriptionManagerFactory newSubscriptionManagerFactory;

    @Test
    public void shouldCreateNewSubscriptionManagerForSpecifiedComponent() throws Exception {

        final String componentName = "some-component";
        final SubscriptionEventProcessor subscriptionEventProcessor = mock(SubscriptionEventProcessor.class);

        when(subscriptionEventProcessorFactory.create(componentName)).thenReturn(subscriptionEventProcessor);

        final SubscriptionManager subscriptionManager = newSubscriptionManagerFactory.create(componentName);

        assertThat(subscriptionManager, is(instanceOf(NewSubscriptionManager.class)));

        assertThat(getValueOfField(subscriptionManager, "bufferedEventFinder", NewEventBufferManager.class), is(newEventBufferManager));
        assertThat(getValueOfField(subscriptionManager, "subscriptionEventProcessor", SubscriptionEventProcessor.class), is(subscriptionEventProcessor));
        assertThat(getValueOfField(subscriptionManager, "componentName", String.class), is(componentName));
    }
}