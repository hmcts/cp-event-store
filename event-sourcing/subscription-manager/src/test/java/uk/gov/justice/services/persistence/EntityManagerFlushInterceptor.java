package uk.gov.justice.services.persistence;

import uk.gov.justice.services.core.interceptor.Interceptor;
import uk.gov.justice.services.core.interceptor.InterceptorChain;
import uk.gov.justice.services.core.interceptor.InterceptorContext;

/**
 * Test-only stand-in that shares the fully-qualified class name of the production
 * {@code uk.gov.justice.services.persistence.EntityManagerFlushInterceptor} (which lives in the framework
 * persistence-jpa module). subscription-manager does not depend on persistence-jpa, so there is no clash — this
 * lets the verifier's by-class-name matching be exercised without pulling in that module.
 */
public class EntityManagerFlushInterceptor implements Interceptor {

    @Override
    public InterceptorContext process(final InterceptorContext interceptorContext, final InterceptorChain interceptorChain) {
        return interceptorChain.processNext(interceptorContext);
    }
}
