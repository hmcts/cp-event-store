package uk.gov.justice.services.eventsourcing.discovery;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSubscriptionDiscovererProducerTest {

    @Mock
    private EventSubscriptionDiscoverer transactionalEventSubscriptionDiscoverer;

    @Mock
    private EventSubscriptionDiscoverer restEventSubscriptionDiscoverer;

    @Mock
    private EventDiscoveryConfig eventDiscoveryConfig;

    @InjectMocks
    private EventSubscriptionDiscovererProducer eventSubscriptionDiscovererProducer;

    @Test
    public void shouldReturnTransactionalDiscovererWhenAccessEventStoreViaRestIsFalse() {

        when(eventDiscoveryConfig.accessEventStoreViaRest()).thenReturn(false);

        assertThat(eventSubscriptionDiscovererProducer.eventSubscriptionDiscoverer(), is(transactionalEventSubscriptionDiscoverer));
    }

    @Test
    public void shouldReturnRestDiscovererWhenAccessEventStoreViaRestIsTrue() {

        when(eventDiscoveryConfig.accessEventStoreViaRest()).thenReturn(true);

        assertThat(eventSubscriptionDiscovererProducer.eventSubscriptionDiscoverer(), is(restEventSubscriptionDiscoverer));
    }
}
