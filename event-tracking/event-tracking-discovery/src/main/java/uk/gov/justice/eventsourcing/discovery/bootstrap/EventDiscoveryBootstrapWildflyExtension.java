package uk.gov.justice.eventsourcing.discovery.bootstrap;

import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventDiscoveryBootstrapWildflyExtension implements Extension {

    private final CdiInstanceResolver cdiInstanceResolver;
    private final Logger logger;

    // Empty constructor required for CDI
    public EventDiscoveryBootstrapWildflyExtension() {
        this(new CdiInstanceResolver(), LoggerFactory.getLogger(EventDiscoveryBootstrapWildflyExtension.class));
    }

    @VisibleForTesting
    public EventDiscoveryBootstrapWildflyExtension(final CdiInstanceResolver cdiInstanceResolver, final Logger logger) {
        this.cdiInstanceResolver = cdiInstanceResolver;
        this.logger = logger;
    }

    public void afterDeploymentValidation(@Observes final AfterDeploymentValidation event, final BeanManager beanManager) {
        logger.info("Bootstrapping Event Discovery...");
        final EventDiscoveryBootstrapper eventDiscoveryBootstrapper = cdiInstanceResolver.getInstanceOf(
                EventDiscoveryBootstrapper.class,
                beanManager);

        eventDiscoveryBootstrapper.bootstrapEventDiscovery();
        logger.info("Event Discovery successfully bootstrapped.");
    }
}
