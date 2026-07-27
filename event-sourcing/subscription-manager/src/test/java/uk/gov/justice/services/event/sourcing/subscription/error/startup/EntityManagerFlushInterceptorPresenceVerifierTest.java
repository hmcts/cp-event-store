package uk.gov.justice.services.event.sourcing.subscription.error.startup;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.core.annotation.Component.COMMAND_HANDLER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_INDEXER;
import static uk.gov.justice.services.core.annotation.Component.EVENT_LISTENER;
import static uk.gov.justice.services.event.sourcing.subscription.error.startup.EntityManagerFlushInterceptorPresenceVerifier.ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME;
import static uk.gov.justice.services.event.sourcing.subscription.error.startup.EntityManagerFlushInterceptorPresenceVerifier.MISSING_INTERCEPTOR_MESSAGE_TEMPLATE;

import uk.gov.justice.services.common.configuration.errors.event.EventErrorHandlingConfiguration;
import uk.gov.justice.services.core.interceptor.Interceptor;
import uk.gov.justice.services.core.interceptor.InterceptorChainEntry;
import uk.gov.justice.services.core.interceptor.InterceptorChainEntryProvider;
import uk.gov.justice.services.framework.utilities.cdi.CdiInstanceResolver;
import uk.gov.justice.services.persistence.EntityManagerFlushInterceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EntityManagerFlushInterceptorPresenceVerifierTest {

    @Mock
    private CdiInstanceResolver cdiInstanceResolver;

    @Mock
    private Logger logger;

    @InjectMocks
    private EntityManagerFlushInterceptorPresenceVerifier verifier;

    // ---- pure decision logic ----

    @Test
    public void shouldNotFlagAnEventListenerChainThatContainsTheFlushInterceptor() {
        final Map<String, Set<String>> byComponent = Map.of(
                EVENT_LISTENER, Set.of(ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME, NonFlushTestInterceptor.class.getName()));

        assertThat(verifier.componentsMissingFlushInterceptor(byComponent), is(empty()));
    }

    @Test
    public void shouldFlagAnEventListenerChainThatIsMissingTheFlushInterceptor() {
        final Map<String, Set<String>> byComponent = Map.of(
                EVENT_LISTENER, Set.of(NonFlushTestInterceptor.class.getName()));

        assertThat(verifier.componentsMissingFlushInterceptor(byComponent), contains(EVENT_LISTENER));
    }

    @Test
    public void shouldFlagCustomEventListenerComponentsThatAreMissingTheFlushInterceptor() {
        final Map<String, Set<String>> byComponent = Map.of(
                "HEARING_EVENT_LISTENER", Set.of(NonFlushTestInterceptor.class.getName()));

        assertThat(verifier.componentsMissingFlushInterceptor(byComponent), contains("HEARING_EVENT_LISTENER"));
    }

    @Test
    public void shouldNotFlagNonEventListenerComponentsEvenWithoutTheFlushInterceptor() {
        final Map<String, Set<String>> byComponent = Map.of(
                COMMAND_HANDLER, Set.of(NonFlushTestInterceptor.class.getName()),
                EVENT_INDEXER, Set.of(NonFlushTestInterceptor.class.getName()));

        assertThat(verifier.componentsMissingFlushInterceptor(byComponent), is(empty()));
    }

    @Test
    public void shouldReportOnlyTheOffendingEventListenerComponents() {
        final Map<String, Set<String>> byComponent = Map.of(
                EVENT_LISTENER, Set.of(ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME),
                "HEARING_EVENT_LISTENER", Set.of(NonFlushTestInterceptor.class.getName()));

        assertThat(verifier.componentsMissingFlushInterceptor(byComponent), contains("HEARING_EVENT_LISTENER"));
    }

    // ---- deployment-time behaviour ----

    @Test
    public void shouldDoNothingWhenSelfHealingIsDisabled() {
        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = mock(BeanManager.class);
        final EventErrorHandlingConfiguration configuration = mock(EventErrorHandlingConfiguration.class);

        when(cdiInstanceResolver.getInstanceOf(EventErrorHandlingConfiguration.class, beanManager)).thenReturn(configuration);
        when(configuration.isEventStreamSelfHealingEnabled()).thenReturn(false);

        verifier.afterDeploymentValidation(event, beanManager);

        verify(beanManager, never()).getBeans(InterceptorChainEntryProvider.class);
        verify(event, never()).addDeploymentProblem(any());
        verifyNoInteractions(logger);
    }

    @Test
    public void shouldNotFailDeploymentWhenFlushInterceptorIsPresentOnEveryEventListenerChain() {
        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = beanManagerWith(event,
                provider(EVENT_LISTENER, EntityManagerFlushInterceptor.class, NonFlushTestInterceptor.class));

        verifier.afterDeploymentValidation(event, beanManager);

        verify(event, never()).addDeploymentProblem(any());
        verifyNoInteractions(logger);
    }

    @Test
    public void shouldFailDeploymentAndLogErrorWhenFlushInterceptorIsMissing() {
        final AfterDeploymentValidation event = mock(AfterDeploymentValidation.class);
        final BeanManager beanManager = beanManagerWith(event,
                provider(EVENT_LISTENER, NonFlushTestInterceptor.class));

        verifier.afterDeploymentValidation(event, beanManager);

        final String expectedMessage = format(
                MISSING_INTERCEPTOR_MESSAGE_TEMPLATE, EVENT_LISTENER, ENTITY_MANAGER_FLUSH_INTERCEPTOR_CLASS_NAME);

        verify(logger).error(expectedMessage);

        final ArgumentCaptor<Throwable> problemCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(event).addDeploymentProblem(problemCaptor.capture());
        assertThat(problemCaptor.getValue(), is(instanceOf(EntityManagerFlushInterceptorMissingException.class)));
        assertThat(problemCaptor.getValue().getMessage(), is(expectedMessage));
    }

    private BeanManager beanManagerWith(final AfterDeploymentValidation event, final InterceptorChainEntryProvider provider) {
        final BeanManager beanManager = mock(BeanManager.class);
        final EventErrorHandlingConfiguration configuration = mock(EventErrorHandlingConfiguration.class);
        final Bean<?> providerBean = mock(Bean.class);
        final CreationalContext<?> creationalContext = mock(CreationalContext.class);

        when(cdiInstanceResolver.getInstanceOf(EventErrorHandlingConfiguration.class, beanManager)).thenReturn(configuration);
        when(configuration.isEventStreamSelfHealingEnabled()).thenReturn(true);
        when(beanManager.getBeans(InterceptorChainEntryProvider.class)).thenReturn(Set.of(providerBean));
        doReturn(creationalContext).when(beanManager).createCreationalContext(providerBean);
        doReturn(provider).when(beanManager).getReference(providerBean, InterceptorChainEntryProvider.class, creationalContext);

        return beanManager;
    }

    @SafeVarargs
    private InterceptorChainEntryProvider provider(final String component, final Class<? extends Interceptor>... interceptorTypes) {
        final List<InterceptorChainEntry> entries = new ArrayList<>();
        int priority = 1;
        for (final Class<? extends Interceptor> interceptorType : interceptorTypes) {
            entries.add(new InterceptorChainEntry(priority++, interceptorType));
        }
        return new InterceptorChainEntryProvider() {
            @Override
            public String component() {
                return component;
            }

            @Override
            public List<InterceptorChainEntry> interceptorChainTypes() {
                return entries;
            }
        };
    }
}
