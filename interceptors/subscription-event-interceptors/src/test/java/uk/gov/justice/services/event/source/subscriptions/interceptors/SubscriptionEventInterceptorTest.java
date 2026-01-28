package uk.gov.justice.services.event.source.subscriptions.interceptors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.core.interceptor.InterceptorChain;
import uk.gov.justice.services.core.interceptor.InterceptorContext;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.subscription.ProcessedEventTrackingService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionEventInterceptorTest {

    @Mock
    private ProcessedEventTrackingService processedEventTrackingService;

    @Mock
    private EventPullConfiguration eventPullConfiguration;

    @InjectMocks
    private SubscriptionEventInterceptor subscriptionEventInterceptor;

    @Test
    public void shouldUpdateCurrentEventNumberIfNewEventNumberGreaterThanCurrentEventNumber() {

        final String componentName = "EVENT_LISTENER";

        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChain interceptorChain = mock(InterceptorChain.class);
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(interceptorChain.processNext(interceptorContext)).thenReturn(interceptorContext);
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(false);
        when(interceptorContext.inputEnvelope()).thenReturn(jsonEnvelope);
        when(interceptorContext.getComponentName()).thenReturn(componentName);

        final InterceptorContext resultInterceptorContext = subscriptionEventInterceptor.process(interceptorContext, interceptorChain);

        assertThat(resultInterceptorContext, is(interceptorContext));

        verify(processedEventTrackingService).trackProcessedEvent(jsonEnvelope, componentName);
    }

    @Test
    public void shouldNotCallProcessedEventTrackingServiceIfPullMechanismEnabled() {

        final InterceptorContext interceptorContext = mock(InterceptorContext.class);
        final InterceptorChain interceptorChain = mock(InterceptorChain.class);

        when(interceptorChain.processNext(interceptorContext)).thenReturn(interceptorContext);
        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);

        final InterceptorContext resultInterceptorContext = subscriptionEventInterceptor.process(interceptorContext, interceptorChain);

        assertThat(resultInterceptorContext, is(interceptorContext));

        verifyNoInteractions(processedEventTrackingService);
    }
}
