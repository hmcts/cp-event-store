package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;

import javax.inject.Inject;

public class ComponentEventProcessor {

    @Inject
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    public void process(final JsonEnvelope eventJsonEnvelope, final String component) {
        final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer.produceLocalProcessor(component);
        final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(eventJsonEnvelope);
        interceptorChainProcessor.process(interceptorContext);
    }
}
