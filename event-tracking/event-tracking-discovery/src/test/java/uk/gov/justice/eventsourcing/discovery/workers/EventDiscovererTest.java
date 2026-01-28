package uk.gov.justice.eventsourcing.discovery.workers;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;
import uk.gov.justice.subscription.SourceComponentPair;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EventDiscovererTest {
    
    @Mock
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Mock
    private Logger logger;

    @InjectMocks
    private EventDiscoverer eventDiscoverer;

    @Test
    public void shouldGetTheLatestPositions() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final UUID latestKnownEventId = randomUUID();
        final UUID streamId_1 = fromString("1111ef5f-2b26-42a2-a01c-4591d2911111");
        final UUID streamId_2 = fromString("2222ef5f-2b26-42a2-a01c-4591d2912222");
        final Long positionInStream_1 = 111L;
        final Long positionInStream_2 = 222L;

        final SourceComponentPair sourceComponentPair = new SourceComponentPair(
                source,
                component);

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);

        final EventSubscriptionStatus eventSubscriptionStatus = mock(EventSubscriptionStatus.class);

        when(eventSubscriptionStatusRepository.findBy( source, component)).thenReturn(of(eventSubscriptionStatus));
        when(eventSubscriptionStatus.latestEventId()).thenReturn(of(latestKnownEventId));
        when(eventSubscriptionDiscoveryBean.discoverNewEvents(of(latestKnownEventId))).thenReturn(asList(streamPosition_1, streamPosition_2));

        when(streamPosition_1.streamId()).thenReturn(streamId_1);
        when(streamPosition_1.positionInStream()).thenReturn(positionInStream_1);

        when(streamPosition_2.streamId()).thenReturn(streamId_2);
        when(streamPosition_2.positionInStream()).thenReturn(positionInStream_2);

        eventDiscoverer.runEventDiscoveryForSourceComponentPair(sourceComponentPair);

        verify(logger).info("Running event discovery for source 'some-source' component 'some-component'");
        verify(newStreamStatusRepository).updateLatestKnownPosition(
                streamId_1,
                source,
                component,
                positionInStream_1);
        verify(logger).info("Updating latest known position to '111' for stream id '1111ef5f-2b26-42a2-a01c-4591d2911111', source 'some-source' component 'some-component'");
        verify(newStreamStatusRepository).updateLatestKnownPosition(
                streamId_2,
                source,
                component,
                positionInStream_2);
        verify(logger).info("Updating latest known position to '222' for stream id '2222ef5f-2b26-42a2-a01c-4591d2912222', source 'some-source' component 'some-component'");
    }

    @Test
    public void shouldDoNothingNoEventSubscriptionStatusFoundForSourceComponent() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final SourceComponentPair sourceComponentPair = new SourceComponentPair(
                source,
                component);

        when(eventSubscriptionStatusRepository.findBy(source, component)).thenReturn(empty());

        eventDiscoverer.runEventDiscoveryForSourceComponentPair(sourceComponentPair);

        verifyNoInteractions(eventSubscriptionDiscoveryBean);
    }
}