package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventBufferProcessor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventBufferProcessorFactoryTest {

    @Mock
    private EventBufferService eventBufferService;

    @Mock
    private SubscriptionEventProcessorFactory subscriptionEventProcessorFactory;

    @InjectMocks
    private EventBufferProcessorFactory eventBufferProcessorFactory;

    @Test
    public void shouldCreateEventBufferProcessor() throws Exception {

        final String componentName = "some-component";

        final SubscriptionEventProcessor subscriptionEventProcessor = mock(SubscriptionEventProcessor.class);

        when(subscriptionEventProcessorFactory.create(componentName)).thenReturn(subscriptionEventProcessor);

        final EventBufferProcessor eventBufferProcessor = eventBufferProcessorFactory.create(componentName);

        assertThat(getValueOfField(eventBufferProcessor, "eventBufferService", EventBufferService.class), is(eventBufferService));
        assertThat(getValueOfField(eventBufferProcessor, "subscriptionEventProcessor", SubscriptionEventProcessor.class), is(subscriptionEventProcessor));
        assertThat(getValueOfField(eventBufferProcessor, "component", String.class), is(componentName));
    }
}