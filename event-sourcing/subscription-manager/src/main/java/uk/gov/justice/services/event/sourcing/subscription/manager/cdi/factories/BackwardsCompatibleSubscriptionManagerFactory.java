package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.event.sourcing.subscription.manager.BackwardsCompatibleSubscriptionManager;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;

import jakarta.inject.Inject;

public class BackwardsCompatibleSubscriptionManagerFactory {

    @Inject
    private InterceptorContextProvider interceptorContextProvider;

    @Inject
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    public BackwardsCompatibleSubscriptionManager create(final String componentName) {

        final InterceptorChainProcessor interceptorChainProcessor = interceptorChainProcessorProducer
                .produceLocalProcessor(componentName);

        return new BackwardsCompatibleSubscriptionManager(
                interceptorChainProcessor,
                interceptorContextProvider);
    }
}
