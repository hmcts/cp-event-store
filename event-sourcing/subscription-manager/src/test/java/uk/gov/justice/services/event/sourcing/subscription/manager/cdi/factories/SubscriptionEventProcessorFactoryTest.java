package uk.gov.justice.services.event.sourcing.subscription.manager.cdi.factories;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.core.interceptor.InterceptorChainProcessor;
import uk.gov.justice.services.core.interceptor.InterceptorChainProcessorProducer;
import uk.gov.justice.services.event.buffer.core.repository.streamerror.StreamStatusErrorPersistence;
import uk.gov.justice.services.event.sourcing.subscription.error.StreamProcessingFailureHandler;
import uk.gov.justice.services.event.sourcing.subscription.error.SubscriptionEventProcessor;
import uk.gov.justice.services.event.sourcing.subscription.manager.cdi.InterceptorContextProvider;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionEventProcessorFactoryTest {

    @Mock
    private InterceptorContextProvider interceptorContextProvider;

    @Mock
    private StreamProcessingFailureHandler streamProcessingFailureHandler;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private StreamStatusErrorPersistence streamStatusErrorPersistence;

    @Mock
    private InterceptorChainProcessorProducer interceptorChainProcessorProducer;

    @InjectMocks
    private SubscriptionEventProcessorFactory subscriptionEventProcessorFactory;

    @Test
    public void shouldCreateNewSubscriptionEventProcessor() throws Exception {

        final String componentName = "some-component";

        final InterceptorChainProcessor interceptorChainProcessor = mock(InterceptorChainProcessor.class);

        when(interceptorChainProcessorProducer.produceLocalProcessor(componentName)).thenReturn(interceptorChainProcessor);

        final SubscriptionEventProcessor subscriptionEventProcessor = subscriptionEventProcessorFactory.create(componentName);

        assertThat(getValueOfField(subscriptionEventProcessor, "interceptorContextProvider", InterceptorContextProvider.class), is(interceptorContextProvider));
        assertThat(getValueOfField(subscriptionEventProcessor, "interceptorChainProcessor", InterceptorChainProcessor.class), is(interceptorChainProcessor));
        assertThat(getValueOfField(subscriptionEventProcessor, "viewStoreJdbcDataSourceProvider", ViewStoreJdbcDataSourceProvider.class), is(viewStoreJdbcDataSourceProvider));
        assertThat(getValueOfField(subscriptionEventProcessor, "streamProcessingFailureHandler", StreamProcessingFailureHandler.class), is(streamProcessingFailureHandler));
        assertThat(getValueOfField(subscriptionEventProcessor, "streamStatusErrorPersistence", StreamStatusErrorPersistence.class), is(streamStatusErrorPersistence));
    }
}