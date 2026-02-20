package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ComponentEventProcessorTest {

    @Mock
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Mock
    private InterceptorContextProvider interceptorContextProvider;

    @InjectMocks
    private ComponentEventProcessor componentEventProcessor;

    @Test
    public void shouldProcessEventThroughInterceptorChain() {
        final String component = "some-component";
        final JsonEnvelope eventJsonEnvelope = mock(JsonEnvelope.class);
        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);
        final InterceptorContext interceptorContext = mock(InterceptorContext.class);

        when(interceptorChainProcessorProducer.produceLocalProcessor(component)).thenReturn(interceptorChainProcessor);
        when(interceptorContextProvider.getInterceptorContext(eventJsonEnvelope)).thenReturn(interceptorContext);

        componentEventProcessor.process(eventJsonEnvelope, component);

        final InOrder inOrder = inOrder(interceptorChainProcessorProducer, interceptorContextProvider, interceptorChainProcessor);
        inOrder.verify(interceptorChainProcessorProducer).produceLocalProcessor(component);
        inOrder.verify(interceptorContextProvider).getInterceptorContext(eventJsonEnvelope);
        inOrder.verify(interceptorChainProcessor).process(interceptorContext);
    }
}
