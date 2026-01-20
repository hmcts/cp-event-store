package uk.gov.justice.eventsourcing.discovery.bootstrap;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;

import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.BeanManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryBootstrapWildflyExtensionTest {

    @Mock
    private CdiInstanceResolver cdiInstanceResolver;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventDiscoveryBootstrapWildflyExtension eventDiscoveryBootstrapWildflyExtension;

    @Test
    public void shouldBootstrapEventDiscovery() throws Exception {

        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = mock(BeanManager.class);
         final EventDiscoveryBootstrapper eventDiscoveryBootstrapper = mock(EventDiscoveryBootstrapper.class);

        when(cdiInstanceResolver.getInstanceOf(
                       EventDiscoveryBootstrapper.class,
                       beanManager)).thenReturn(eventDiscoveryBootstrapper);
        
        eventDiscoveryBootstrapWildflyExtension.afterDeploymentValidation(event, beanManager);

        final InOrder inOrder = inOrder(logger, eventDiscoveryBootstrapper);

        inOrder.verify(logger).info("Bootstrapping Event Discovery...");
        inOrder.verify(eventDiscoveryBootstrapper).bootstrapEventDiscovery();
        inOrder.verify(logger).info("Event Discovery successfully bootstrapped.");

    }
}