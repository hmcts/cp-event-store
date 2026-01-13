package uk.gov.justice.eventsourcing.discovery.workers;

import static java.util.Arrays.asList;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.eventsourcing.discovery.subscription.SourceComponentPair;
import uk.gov.justice.eventsourcing.discovery.subscription.SubscriptionSourceComponentFinder;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscovererTest {
    
    @Mock
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @InjectMocks
    private EventDiscoverer eventDiscoverer;

    @Test
    public void shouldGetTheLatestPositions() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final UUID latestKnownEventId = randomUUID();
        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final Long positionInStream_1 = 111L;
        final Long positionInStream_2 = 222L;

        final SourceComponentPair sourceComponentPair = new SourceComponentPair(
                source,
                component);

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);

        final EventSubscriptionStatus eventSubscriptionStatus = mock(EventSubscriptionStatus.class);

        when(eventSubscriptionStatusRepository.findBy( source, component)).thenReturn(of(eventSubscriptionStatus));
        when(eventSubscriptionStatus.latestEventId()).thenReturn(latestKnownEventId);
        when(eventSubscriptionDiscoveryBean.discoverNewEvents(latestKnownEventId)).thenReturn(asList(streamPosition_1, streamPosition_2));

        when(streamPosition_1.streamId()).thenReturn(streamId_1);
        when(streamPosition_1.positionInStream()).thenReturn(positionInStream_1);

        when(streamPosition_2.streamId()).thenReturn(streamId_2);
        when(streamPosition_2.positionInStream()).thenReturn(positionInStream_2);

        eventDiscoverer.runEventDiscoveryForSourceComponentPair(sourceComponentPair);

        verify(newStreamStatusRepository).updateLatestKnownPosition(
                streamId_1,
                source,
                component,
                positionInStream_1);
        verify(newStreamStatusRepository).updateLatestKnownPosition(
                streamId_2,
                source,
                component,
                positionInStream_2);
    }
}