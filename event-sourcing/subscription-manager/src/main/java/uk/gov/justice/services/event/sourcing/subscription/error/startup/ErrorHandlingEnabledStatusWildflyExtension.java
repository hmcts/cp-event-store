package uk.gov.justice.services.event.sourcing.subscription.error.startup;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.Extension;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlingEnabledStatusWildflyExtension implements Extension {

    private final CdiInstanceResolver cdiInstanceResolver;
    private final Logger logger;

    // Empty constructor required for CDI
    public ErrorHandlingEnabledStatusWildflyExtension() {
        this(new CdiInstanceResolver(), LoggerFactory.getLogger(ErrorHandlingEnabledStatusWildflyExtension.class));
    }

    @VisibleForTesting
    public ErrorHandlingEnabledStatusWildflyExtension(final CdiInstanceResolver cdiInstanceResolver, final Logger logger) {
        this.cdiInstanceResolver = cdiInstanceResolver;
        this.logger = logger;
    }

    public void afterDeploymentValidation(@Observes final AfterDeploymentValidation event, final BeanManager beanManager) {

        final EventErrorHandlingConfiguration eventErrorHandlingConfiguration = cdiInstanceResolver.getInstanceOf(
                EventErrorHandlingConfiguration.class,
                beanManager);

        if (eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()) {
            logger.warn("Event Error Handling/Self Healing is enabled for this context");
        } else {
            logger.warn("Event Error Handling/Self Healing is disabled for this context");
        }
    }
}
