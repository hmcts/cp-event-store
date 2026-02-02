package uk.gov.justice.subscription;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.Subscription;
import uk.gov.justice.subscription.domain.subscriptiondescriptor.SubscriptionsDescriptor;
import uk.gov.justice.subscription.registry.SubscriptionsDescriptorsRegistry;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SubscriptionSourceComponentFinderTest {

    @Mock
    private SubscriptionsDescriptorsRegistry subscriptionsDescriptorsRegistry;

    @InjectMocks
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Test
    public void shouldFindAllSourceComponentPairsInSubscriptionsRegistry() throws Exception {

        final String source_1 = "source_1";
        final String source_2 = "source_2";
        final String component_1 = "component_1";
        final String component_2 = "component_2";

        final SubscriptionsDescriptor subscriptionsDescriptor_1 = mock(SubscriptionsDescriptor.class);
        final SubscriptionsDescriptor subscriptionsDescriptor_2 = mock(SubscriptionsDescriptor.class);

        final Subscription subscription_1_1 = mock(Subscription.class);
        final Subscription subscription_1_2 = mock(Subscription.class);
        final Subscription subscription_2_1 = mock(Subscription.class);
        final Subscription subscription_2_2 = mock(Subscription.class);


        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(
                subscriptionsDescriptor_1,
                subscriptionsDescriptor_2
        ));

        when(subscriptionsDescriptor_1.getServiceComponent()).thenReturn(component_1);
        when(subscriptionsDescriptor_2.getServiceComponent()).thenReturn(component_2);

        when(subscriptionsDescriptor_1.getSubscriptions()).thenReturn(asList(
                subscription_1_1,
                subscription_1_2
        ));
        when(subscriptionsDescriptor_2.getSubscriptions()).thenReturn(asList(
                subscription_2_1,
                subscription_2_2
        ));

        when(subscription_1_1.getEventSourceName()).thenReturn(source_1);
        when(subscription_1_2.getEventSourceName()).thenReturn(source_2);
        when(subscription_2_1.getEventSourceName()).thenReturn(source_1);
        when(subscription_2_2.getEventSourceName()).thenReturn(source_2);

        final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                .findSourceComponentPairsFromSubscriptionRegistry();

        assertThat(sourceComponentPairs.size(), is(4));

        assertThat(sourceComponentPairs.get(0).source(), is(source_1));
        assertThat(sourceComponentPairs.get(0).component(), is(component_1));

        assertThat(sourceComponentPairs.get(1).source(), is(source_2));
        assertThat(sourceComponentPairs.get(1).component(), is(component_1));

        assertThat(sourceComponentPairs.get(2).source(), is(source_1));
        assertThat(sourceComponentPairs.get(2).component(), is(component_2));

        assertThat(sourceComponentPairs.get(3).source(), is(source_2));
        assertThat(sourceComponentPairs.get(3).component(), is(component_2));
    }

    @Test
    public void shouldCacheResultsFromSubscriptionsRegistry() throws Exception {

        final String source_1 = "source_1";
        final String source_2 = "source_2";
        final String component_1 = "component_1";
        final String component_2 = "component_2";

        final SubscriptionsDescriptor subscriptionsDescriptor_1 = mock(SubscriptionsDescriptor.class);
        final SubscriptionsDescriptor subscriptionsDescriptor_2 = mock(SubscriptionsDescriptor.class);

        final Subscription subscription_1_1 = mock(Subscription.class);
        final Subscription subscription_1_2 = mock(Subscription.class);
        final Subscription subscription_2_1 = mock(Subscription.class);
        final Subscription subscription_2_2 = mock(Subscription.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(
                subscriptionsDescriptor_1,
                subscriptionsDescriptor_2
        ));

        when(subscriptionsDescriptor_1.getServiceComponent()).thenReturn(component_1);
        when(subscriptionsDescriptor_2.getServiceComponent()).thenReturn(component_2);

        when(subscriptionsDescriptor_1.getSubscriptions()).thenReturn(asList(
                subscription_1_1,
                subscription_1_2
        ));
        when(subscriptionsDescriptor_2.getSubscriptions()).thenReturn(asList(
                subscription_2_1,
                subscription_2_2
        ));

        when(subscription_1_1.getEventSourceName()).thenReturn(source_1);
        when(subscription_1_2.getEventSourceName()).thenReturn(source_2);
        when(subscription_2_1.getEventSourceName()).thenReturn(source_1);
        when(subscription_2_2.getEventSourceName()).thenReturn(source_2);


        final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                .findSourceComponentPairsFromSubscriptionRegistry();

        assertThat(sourceComponentPairs.size(), is(4));

        verify(subscriptionsDescriptorsRegistry, times(1)).getAll();

        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();
        subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry();

        verify(subscriptionsDescriptorsRegistry, times(1)).getAll();
    }


    @Test
    public void shouldFindOnlyListenerOrIndexerSourceComponentPairs() throws Exception {

        final String source_1 = "source_1";
        final String component_listener = "something_EVENT_LISTENER";
        final String component_indexer = "something_EVENT_INDEXER";
        final String component_other = "something_EVENT_PROCESSOR";

        final SubscriptionsDescriptor subscriptionsDescriptor_listener = mock(SubscriptionsDescriptor.class);
        final SubscriptionsDescriptor subscriptionsDescriptor_indexer = mock(SubscriptionsDescriptor.class);
        final SubscriptionsDescriptor subscriptionsDescriptor_other = mock(SubscriptionsDescriptor.class);

        final Subscription subscription_1 = mock(Subscription.class);
        final Subscription subscription_2 = mock(Subscription.class);
        final Subscription subscription_3 = mock(Subscription.class);

        when(subscriptionsDescriptorsRegistry.getAll()).thenReturn(asList(
                subscriptionsDescriptor_listener,
                subscriptionsDescriptor_indexer,
                subscriptionsDescriptor_other
        ));

        when(subscriptionsDescriptor_listener.getServiceComponent()).thenReturn(component_listener);
        when(subscriptionsDescriptor_indexer.getServiceComponent()).thenReturn(component_indexer);
        when(subscriptionsDescriptor_other.getServiceComponent()).thenReturn(component_other);

        when(subscriptionsDescriptor_listener.getSubscriptions()).thenReturn(asList(subscription_1));
        when(subscriptionsDescriptor_indexer.getSubscriptions()).thenReturn(asList(subscription_2));
        when(subscriptionsDescriptor_other.getSubscriptions()).thenReturn(asList(subscription_3));

        when(subscription_1.getEventSourceName()).thenReturn(source_1);
        when(subscription_2.getEventSourceName()).thenReturn(source_1);
        when(subscription_3.getEventSourceName()).thenReturn(source_1);

        final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                .findListenerOrIndexerPairs();

        assertThat(sourceComponentPairs.size(), is(2));

        assertThat(sourceComponentPairs.get(0).source(), is(source_1));
        assertThat(sourceComponentPairs.get(0).component(), is(component_listener));

        assertThat(sourceComponentPairs.get(1).source(), is(source_1));
        assertThat(sourceComponentPairs.get(1).component(), is(component_indexer));
    }

}