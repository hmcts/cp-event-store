package uk.gov.justice.services.event.sourcing.subscription.error.startup;

import uk.gov.justice.services.core.interceptor.Interceptor;
import uk.gov.justice.services.core.interceptor.InterceptorChain;
import uk.gov.justice.services.core.interceptor.InterceptorContext;

/**
 * An arbitrary interceptor that is NOT the EntityManagerFlushInterceptor, used to populate event-listener chains
 * that are missing the flush interceptor in verifier tests.
 */
public class NonFlushTestInterceptor implements Interceptor {

    @Override
    public InterceptorContext process(final InterceptorContext interceptorContext, final InterceptorChain interceptorChain) {
        return interceptorChain.processNext(interceptorContext);
    }
}
