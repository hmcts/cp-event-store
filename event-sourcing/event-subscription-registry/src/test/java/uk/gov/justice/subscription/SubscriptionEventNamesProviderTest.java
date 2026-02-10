package uk.gov.justice.subscription;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.subscription.domain.subscriptiondescriptor.Event;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SubscriptionEventNamesProviderTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @InjectMocks
    private SubscriptionEventNamesProvider subscriptionEventNamesProvider;

    @Test
    public void shouldAcceptMultipleEventNamesForSourceAndComponent() {

        final String source = "example";
        final String component = "EVENT_LISTENER";

        final Event event1 = new Event("example.recipe-added", "http://example.com/schema1");
        final Event event2 = new Event("example.recipe-deleted", "http://example.com/schema2");

        final Subscription subscription = mock(Subscription.class);
        final SubscriptionsDescriptor descriptor = mock(SubscriptionsDescriptor.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(descriptor));
        when(descriptor.getServiceComponent()).thenReturn(component);
        when(descriptor.getSubscriptions()).thenReturn(singletonList(subscription));
        when(subscription.getEventSourceName()).thenReturn(source);
        when(subscription.getEvents()).thenReturn(asList(event1, event2));

        subscriptionEventNamesProvider.init();

        assertThat(subscriptionEventNamesProvider.accepts("example.recipe-added", source, component), is(true));
        assertThat(subscriptionEventNamesProvider.accepts("example.recipe-deleted", source, component), is(true));
    }

    @Test
    public void shouldAcceptEventNameThatIsInTheSubscriptionDescriptor() {

        final String source = "example";
        final String component = "EVENT_LISTENER";

        final Event event = new Event("example.recipe-added", "http://example.com/schema1");

        final Subscription subscription = mock(Subscription.class);
        final SubscriptionsDescriptor descriptor = mock(SubscriptionsDescriptor.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(descriptor));
        when(descriptor.getServiceComponent()).thenReturn(component);
        when(descriptor.getSubscriptions()).thenReturn(singletonList(subscription));
        when(subscription.getEventSourceName()).thenReturn(source);
        when(subscription.getEvents()).thenReturn(singletonList(event));

        subscriptionEventNamesProvider.init();

        assertThat(subscriptionEventNamesProvider.accepts("example.recipe-added", source, component), is(true));
        assertThat(subscriptionEventNamesProvider.accepts("example.unknown-event", source, component), is(false));
    }

    @Test
    public void shouldNotAcceptEventNameForUnknownSourceComponentPair() {

        final Event event = new Event("example.recipe-added", "http://example.com/schema1");

        final Subscription subscription = mock(Subscription.class);
        final SubscriptionsDescriptor descriptor = mock(SubscriptionsDescriptor.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(descriptor));
        when(descriptor.getServiceComponent()).thenReturn("EVENT_LISTENER");
        when(descriptor.getSubscriptions()).thenReturn(singletonList(subscription));
        when(subscription.getEventSourceName()).thenReturn("example");
        when(subscription.getEvents()).thenReturn(singletonList(event));

        subscriptionEventNamesProvider.init();

        assertThat(subscriptionEventNamesProvider.accepts("example.recipe-added", "unknown-source", "UNKNOWN_COMPONENT"), is(false));
    }

    @Test
    public void shouldBuildEventNameMapOnceAtInitialisation() {

        final String source = "example";
        final String component = "EVENT_LISTENER";

        final Event event = new Event("example.recipe-added", "http://example.com/schema1");

        final Subscription subscription = mock(Subscription.class);
        final SubscriptionsDescriptor descriptor = mock(SubscriptionsDescriptor.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(singletonList(descriptor));
        when(descriptor.getServiceComponent()).thenReturn(component);
        when(descriptor.getSubscriptions()).thenReturn(singletonList(subscription));
        when(subscription.getEventSourceName()).thenReturn(source);
        when(subscription.getEvents()).thenReturn(singletonList(event));

        subscriptionEventNamesProvider.init();

        subscriptionEventNamesProvider.accepts("example.recipe-added", source, component);
        subscriptionEventNamesProvider.accepts("example.recipe-added", source, component);
        subscriptionEventNamesProvider.accepts("example.recipe-added", source, component);

        verify(subscriptionsDescriptorsRegistry, times(1)).getAll();
    }
}
