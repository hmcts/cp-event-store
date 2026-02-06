package uk.gov.justice.eventsourcing.discovery.bootstrap;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryBootstrapperTest {

    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Mock
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventDiscoveryBootstrapper eventDiscoveryBootstrapper;

    @Test
    public void shouldInsertEmptyRowsInEventSubscriptionStatusForAnyMissingSourceComponentPair() throws Exception {

        final String source_1 = "source_1";
        final String source_2 = "source_2";
        final String component_1 = "component_1";
        final String component_2 = "component_2";

        final SourceComponentPair sourceComponentPair_1 = new SourceComponentPair(source_1, component_1);
        final SourceComponentPair sourceComponentPair_2 = new SourceComponentPair(source_2, component_2);

        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs())
                .thenReturn(asList(sourceComponentPair_1, sourceComponentPair_2));

        when(eventSubscriptionStatusRepository.findBy(source_1, component_1)).thenReturn(of(mock(EventSubscriptionStatus.class)));
        when(eventSubscriptionStatusRepository.findBy(source_2, component_2)).thenReturn(empty());

        eventDiscoveryBootstrapper.bootstrapEventDiscovery();

        final InOrder inOrder = inOrder(eventSubscriptionStatusRepository, logger);
        inOrder.verify(eventSubscriptionStatusRepository).insertEmptyRowFor(source_2, component_2);
        inOrder.verify(logger).info("Inserted empty row into event_subscription_status for source 'source_2', component 'component_2'");

        verify(eventSubscriptionStatusRepository, never()).insertEmptyRowFor(source_1, component_1);
    }
}