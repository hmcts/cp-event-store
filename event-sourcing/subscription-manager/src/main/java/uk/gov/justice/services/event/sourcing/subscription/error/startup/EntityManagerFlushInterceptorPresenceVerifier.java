package uk.gov.justice.services.event.sourcing.subscription.error.startup;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.core.interceptor.InterceptorChainEntryProvider;
import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fails the deployment if event stream self-healing is enabled but the {@code EntityManagerFlushInterceptor}
 * is absent from an event-listener interceptor chain.
 *
 * <p>The flush interceptor forces Hibernate insert/update errors to surface <em>inside</em> the interceptor
 * chain, where the self-healing error handling can catch and record them. If it is missing, those errors only
 * fire at container commit (outside application code) and are never recorded — silently breaking error capture.
 * It has been accidentally dropped more than once (a persistence dependency reworked off the classpath, or a
 * custom event-listener component that builds its own chain), and the failure is invisible until an error needs
 * to be captured and isn't. This verifier turns that silent gap into a hard deployment failure.</p>
 *
 * <p>The interceptor is matched by fully-qualified <strong>class name</strong>, deliberately, so this check still
 * runs (and fails) when the module providing the interceptor has been dropped from the deployment entirely.</p>
 *
 * <p>The check is gated on {@link EventErrorHandlingConfiguration#isEventStreamSelfHealingEnabled()}: when
 * self-healing is off the flush interceptor is intentionally not on the chain, so no verification is performed.</p>
 */
public class EntityManagerFlushInterceptorPresenceVerifier implements Extension {

    @VisibleForTesting
    static final String ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME =
            "uk.gov.justice.services.persistence.EntityManagerFlushInterceptor";

    @VisibleForTesting
    static final String MISSING_INTERCEPTOR_MESSAGE_TEMPLATE = """
            Deployment stopped: event stream self-healing (error handling) is switched ON for this context, but the \
            EntityManagerFlushInterceptor is not on the interceptor chain for event-listener component(s): %s.

            Why this is fatal: this interceptor forces each event-listener's database changes to be written while the \
            event is still being handled. That is what lets self-healing catch a database error, record it in the error \
            tables, and retry the event. Without it, database errors only happen later - after the handler has finished - \
            where self-healing cannot see them, so failing events are silently dropped instead of recorded and retried.

            How to fix: ensure the framework 'persistence-jpa' module is on this deployment, and that every \
            event-listener component registers the interceptor (%s) on its chain. If self-healing is intentionally off, \
            this check does not run.""";

    private final CdiInstanceResolver cdiInstanceResolver;
    private final Logger logger;

    // Empty constructor required for CDI
    public EntityManagerFlushInterceptorPresenceVerifier() {
        this(new CdiInstanceResolver(), LoggerFactory.getLogger(EntityManagerFlushInterceptorPresenceVerifier.class));
    }

    @VisibleForTesting
    public EntityManagerFlushInterceptorPresenceVerifier(final CdiInstanceResolver cdiInstanceResolver, final Logger logger) {
        this.cdiInstanceResolver = cdiInstanceResolver;
        this.logger = logger;
    }

    public void afterDeploymentValidation(@Observes final AfterDeploymentValidation event, final BeanManager beanManager) {

        final EventErrorHandlingConfiguration eventErrorHandlingConfiguration = cdiInstanceResolver.getInstanceOf(
                EventErrorHandlingConfiguration.class,
                beanManager);

        if (!eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()) {
            return;
        }

        final Map<String, Set<String>> interceptorClassNamesByComponent = interceptorClassNamesByComponent(beanManager);

        final List<String> componentsMissingFlushInterceptor = componentsMissingFlushInterceptor(interceptorClassNamesByComponent);

        if (!componentsMissingFlushInterceptor.isEmpty()) {
            final String message = format(
                    MISSING_INTERCEPTOR_MESSAGE_TEMPLATE,
                    String.join(", ", componentsMissingFlushInterceptor),
                    ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME);

            logger.error(message);
            event.addDeploymentProblem(new EntityManagerFlushInterceptorMissingException(message));
        }
    }

    private Map<String, Set<String>> interceptorClassNamesByComponent(final BeanManager beanManager) {

        final Map<String, Set<String>> interceptorClassNamesByComponent = new HashMap<>();

        for (final Bean<?> providerBean : beanManager.getBeans(InterceptorChainEntryProvider.class)) {
            final CreationalContext<?> creationalContext = beanManager.createCreationalContext(providerBean);
            final InterceptorChainEntryProvider provider = (InterceptorChainEntryProvider) beanManager.getReference(
                    providerBean,
                    InterceptorChainEntryProvider.class,
                    creationalContext);

            final Set<String> interceptorClassNames = interceptorClassNamesByComponent.computeIfAbsent(
                    provider.component(),
                    component -> new HashSet<>());

            provider.interceptorChainTypes().forEach(entry ->
                    interceptorClassNames.add(entry.getInterceptorType().getName()));
        }

        return interceptorClassNamesByComponent;
    }

    /**
     * Every component whose name identifies it as an event listener (the standard {@code EVENT_LISTENER} and
     * any custom {@code *_EVENT_LISTENER}) must carry the flush interceptor when self-healing is enabled.
     */
    @VisibleForTesting
    List<String> componentsMissingFlushInterceptor(final Map<String, Set<String>> interceptorClassNamesByComponent) {

        return interceptorClassNamesByComponent.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().contains(EVENT_LISTENER))
                .filter(entry -> !entry.getValue().contains(ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME))
                .map(Map.Entry::getKey)
                .sorted()
                .collect(toList());
    }
}
