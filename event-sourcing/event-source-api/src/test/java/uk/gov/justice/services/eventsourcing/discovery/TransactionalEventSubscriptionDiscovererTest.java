package uk.gov.justice.services.eventsourcing.discovery;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventIdNumber;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TransactionalEventSubscriptionDiscovererTest {

    @Mock
    private EventDiscoveryConfig eventDiscoveryConfig;

    @Mock
    private EventDiscoveryRepository eventDiscoveryRepository;

    @InjectMocks
    private TransactionalEventSubscriptionDiscoverer transactionalEventSubscriptionDiscoverer;

    @Test
    public void shouldDiscoverLatestPositionsFromTheEventStoreAndUpdateStreamStatus() throws Exception {

        final UUID latestKnownEventId = randomUUID();
        final long firstEventNumber = 10L;
        final int batchSize = 23;
        final long lastEventNumber = 20L;

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);
        final UUID newLatestEventId = randomUUID();

        when(eventDiscoveryConfig.getBatchSize()).thenReturn(batchSize);
        when(eventDiscoveryRepository.getEventNumberFor(latestKnownEventId)).thenReturn(firstEventNumber);
        when(eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(firstEventNumber, batchSize))
                .thenReturn(of(new EventIdNumber(newLatestEventId, lastEventNumber)));

        when(eventDiscoveryRepository.getLatestStreamPositionsBetween(firstEventNumber, lastEventNumber))
                .thenReturn(asList(
                        streamPosition_1,
                        streamPosition_2)
                );

        final DiscoveryResult discoveryResult = transactionalEventSubscriptionDiscoverer.discoverNewEvents(of(latestKnownEventId));

        assertThat(discoveryResult.streamPositions().size(), is(2));
        assertThat(discoveryResult.streamPositions().get(0), is(streamPosition_1));
        assertThat(discoveryResult.streamPositions().get(1), is(streamPosition_2));
        assertThat(discoveryResult.latestKnownEventId(), is(of(newLatestEventId)));
    }

    @Test
    public void shouldDiscoverLatestPositionsFromZerothEventIfLatestKnownEventIdNotFound() throws Exception {

        final int batchSize = 23;
        final long firstEventNumber = 0L;
        final long lastEventNumber = 20L;

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);
        final UUID newLatestEventId = randomUUID();

        when(eventDiscoveryConfig.getBatchSize()).thenReturn(batchSize);
        when(eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(firstEventNumber, batchSize))
                .thenReturn(of(new EventIdNumber(newLatestEventId, lastEventNumber)));

        when(eventDiscoveryRepository.getLatestStreamPositionsBetween(firstEventNumber, lastEventNumber))
                .thenReturn(asList(
                        streamPosition_1,
                        streamPosition_2)
                );

        final DiscoveryResult discoveryResult = transactionalEventSubscriptionDiscoverer.discoverNewEvents(empty());

        assertThat(discoveryResult.streamPositions().size(), is(2));
        assertThat(discoveryResult.streamPositions().get(0), is(streamPosition_1));
        assertThat(discoveryResult.streamPositions().get(1), is(streamPosition_2));
        assertThat(discoveryResult.latestKnownEventId(), is(of(newLatestEventId)));
    }

    @Test
    public void shouldReturnEmptyDiscoveryResultIfNoNewEventsFound() throws Exception {

        final UUID latestKnownEventId = randomUUID();
        final long firstEventNumber = 10L;
        final int batchSize = 23;

        when(eventDiscoveryConfig.getBatchSize()).thenReturn(batchSize);
        when(eventDiscoveryRepository.getEventNumberFor(latestKnownEventId)).thenReturn(firstEventNumber);
        when(eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(firstEventNumber, batchSize))
                .thenReturn(of(new EventIdNumber(latestKnownEventId, firstEventNumber)));

        final DiscoveryResult discoveryResult = transactionalEventSubscriptionDiscoverer.discoverNewEvents(of(latestKnownEventId));

        assertThat(discoveryResult.streamPositions().isEmpty(), is(true));
        assertThat(discoveryResult.latestKnownEventId(), is(empty()));
    }
}
