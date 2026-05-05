package uk.gov.justice.services.event.sourcing.subscription.error.startup;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;

import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.BeanManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class ErrorHandlingEnabledStatusWildflyExtensionTest {

    @Mock
    private CdiInstanceResolver cdiInstanceResolver;

    @Mock
    private Logger logger;

    @InjectMocks
    private ErrorHandlingEnabledStatusWildflyExtension errorHandlingEnabledStatusWildflyExtension;

    @Test
    public void shouldLogWarningIfEventErrorHandlingIsEnabled() throws Exception {

        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = mock(BeanManager.class);
        final EventErrorHandlingConfiguration eventErrorHandlingConfiguration = mock(EventErrorHandlingConfiguration.class);

        when(cdiInstanceResolver.getInstanceOf(EventErrorHandlingConfiguration.class, beanManager))
                .thenReturn(eventErrorHandlingConfiguration);
        when(eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()).thenReturn(true);

        errorHandlingEnabledStatusWildflyExtension.afterDeploymentValidation(event, beanManager);

        verify(logger).warn("Event Error Handling/Self Healing is enabled for this context");
    }

    @Test
    public void shouldLogWarningIfEventErrorHandlingIsDisabled() throws Exception {

        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = mock(BeanManager.class);
        final EventErrorHandlingConfiguration eventErrorHandlingConfiguration = mock(EventErrorHandlingConfiguration.class);

        when(cdiInstanceResolver.getInstanceOf(EventErrorHandlingConfiguration.class, beanManager))
                .thenReturn(eventErrorHandlingConfiguration);
        when(eventErrorHandlingConfiguration.isEventStreamSelfHealingEnabled()).thenReturn(false);

        errorHandlingEnabledStatusWildflyExtension.afterDeploymentValidation(event, beanManager);

        verify(logger).warn("Event Error Handling/Self Healing is disabled for this context");
    }
}