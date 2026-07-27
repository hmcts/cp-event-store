package uk.gov.justice.services.event.sourcing.subscription.error.startup;

/**
 * Thrown at deployment time when event stream self-healing is enabled but one or more event-listener
 * interceptor chains are missing the {@code EntityManagerFlushInterceptor}. Registered as a CDI deployment
 * problem so WildFly fails the deployment rather than starting a context whose error handling is silently broken.
 */
public class EntityManagerFlushInterceptorMissingException extends RuntimeException {

    public EntityManagerFlushInterceptorMissingException(final String message) {
        super(message);
    }
}
