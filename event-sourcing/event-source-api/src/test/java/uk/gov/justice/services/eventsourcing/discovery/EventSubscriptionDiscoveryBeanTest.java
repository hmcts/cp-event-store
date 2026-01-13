package uk.gov.justice.services.eventsourcing.discovery;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.List;
import java.util.UUID;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventSubscriptionDiscoveryBeanTest {

    @Mock
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Mock
    private EventDiscoveryRepository eventDiscoveryRepository;

    @InjectMocks
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Test
    public void shouldDiscoverLatestPositionsFromTheEventStoreAndUpdateStreamStatus() throws Exception {

        final UUID latestKnownEventId = UUID.randomUUID();
        final int batchSize = 23;

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);

        when(eventDiscoveryConfig.getBatchSize()).thenReturn(batchSize);
        when(eventDiscoveryRepository.getLatestStreamPositions(latestKnownEventId, batchSize))
                .thenReturn(asList(
                        streamPosition_1,
                        streamPosition_2)
                );

        final List<StreamPosition> streamPositions = eventSubscriptionDiscoveryBean.discoverNewEvents(latestKnownEventId);

        assertThat(streamPositions.size(), is(2));
        assertThat(streamPositions.get(0), is(streamPosition_1));
        assertThat(streamPositions.get(1), is(streamPosition_2));
    }
}