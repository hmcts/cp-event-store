package uk.gov.justice.eventsourcing.discovery.workers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryWorkerTest {


    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Mock
    private EventDiscoverer eventDiscoverer;

    @InjectMocks
    private EventDiscoveryWorker eventDiscoveryWorker;

    @Test
    public void shouldFindAllSourceComponentPairsAndRunDiscoveryForEach() throws Exception {

        final SourceComponentPair sourceComponentPair_1 = mock(SourceComponentPair.class);
        final SourceComponentPair sourceComponentPair_2 = mock(SourceComponentPair.class);

        when(subscriptionSourceComponentFinder.findSourceComponentPairsFromSubscriptionRegistry()).thenReturn(
                asList(sourceComponentPair_1, sourceComponentPair_2)
        );

        eventDiscoveryWorker.runEventDiscovery();

        verify(eventDiscoverer).runEventDiscoveryForSourceComponentPair(sourceComponentPair_1);
        verify(eventDiscoverer).runEventDiscoveryForSourceComponentPair(sourceComponentPair_2);
    }
}