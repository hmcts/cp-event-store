package uk.gov.justice.services.event.sourcing.subscription.manager;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.event.buffer.api.EventBufferService;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import java.util.stream.Stream;

import jakarta.inject.Inject;

public class CatchupEventBufferProcessor {

    @Inject
    private EventBufferService eventBufferService;

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    @Inject
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @Inject
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    public void processWithEventBuffer(final JsonEnvelope incomingJsonEnvelope, final String subscriptionName) {
        final String componentName = subscriptionsDescriptorsRegistry.findComponentNameBy(subscriptionName);

        final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer.produceLocalProcessor(componentName);

        try (final Stream<JsonEnvelope> jsonEnvelopeStream = eventBufferService.currentOrderedEventsWith(incomingJsonEnvelope, componentName)) {
            jsonEnvelopeStream.forEach(incomingJsonEnvelope1 -> {

                final InterceptorContext interceptorContext = interceptorContextProvider.getInterceptorContext(incomingJsonEnvelope1);
                interceptorChainProcessor.process(interceptorContext);
            });
        }
    }
}

