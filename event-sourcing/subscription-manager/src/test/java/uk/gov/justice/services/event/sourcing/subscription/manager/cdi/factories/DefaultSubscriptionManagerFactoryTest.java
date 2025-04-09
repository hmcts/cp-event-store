package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.event.sourcing.subscription.manager.DefaultSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.EventBufferProcessor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultSubscriptionManagerFactoryTest {

    @Mock
    private EventBufferProcessorFactory eventBufferProcessorFactory;

    @InjectMocks
    private DefaultSubscriptionManagerFactory defaultSubscriptionManagerFactory;

    @Test
    public void shouldCreateDefaultSubscriptionManagerFactory() throws Exception {

        final String componentName = "componentName";
        final EventBufferProcessor eventBufferProcessor = mock(EventBufferProcessor.class);

        when(eventBufferProcessorFactory.create(componentName)).thenReturn(eventBufferProcessor);

        final DefaultSubscriptionManager defaultSubscriptionManager = (DefaultSubscriptionManager) defaultSubscriptionManagerFactory.create(componentName);
        assertThat(getValueOfField(defaultSubscriptionManager, "eventBufferProcessor", EventBufferProcessor.class), is(eventBufferProcessor));
    }
}
