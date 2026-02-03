package uk.gov.justice.eventsourcing.discovery.workers;

import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatus;
import uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.eventsourcing.discovery.DiscoveryResult;
import uk.gov.justice.services.eventsourcing.discovery.EventSubscriptionDiscoveryBean;
import uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.StreamPosition;
import uk.gov.justice.subscription.SourceComponentPair;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryWorkerTest {
    
    @Mock
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @Mock
    private EventSubscriptionDiscoveryBean eventSubscriptionDiscoveryBean;

    @Mock
    private Logger logger;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private EventDiscoveryWorker eventDiscoveryWorker;

    @Test
    public void shouldGetTheLatestPositions() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final UUID latestKnownEventId = randomUUID();
        final UUID streamId_1 = fromString("1111ef5f-2b26-42a2-a01c-4591d2911111");
        final UUID streamId_2 = fromString("2222ef5f-2b26-42a2-a01c-4591d2912222");
        final Long positionInStream_1 = 111L;
        final Long positionInStream_2 = 222L;
        final ZonedDateTime now = ZonedDateTime.now();

        final SourceComponentPair sourceComponentPair = new SourceComponentPair(
                source,
                component);

        final StreamPosition streamPosition_1 = mock(StreamPosition.class);
        final StreamPosition streamPosition_2 = mock(StreamPosition.class);

        final EventSubscriptionStatus eventSubscriptionStatus = mock(EventSubscriptionStatus.class);

        final UUID newLatestEventId = randomUUID();
        when(eventSubscriptionStatusRepository.findBy( source, component)).thenReturn(of(eventSubscriptionStatus));
        when(eventSubscriptionStatus.latestEventId()).thenReturn(of(latestKnownEventId));
        when(eventSubscriptionDiscoveryBean.discoverNewEvents(of(latestKnownEventId))).thenReturn(new DiscoveryResult(asList(streamPosition_1, streamPosition_2), of(newLatestEventId)));

        when(streamPosition_1.streamId()).thenReturn(streamId_1);
        when(streamPosition_1.positionInStream()).thenReturn(positionInStream_1);

        when(streamPosition_2.streamId()).thenReturn(streamId_2);
        when(streamPosition_2.positionInStream()).thenReturn(positionInStream_2);

        when(clock.now()).thenReturn(now);

        eventDiscoveryWorker.runEventDiscoveryForSourceComponentPair(sourceComponentPair);

        verify(logger).debug("Running event discovery for source 'some-source' component 'some-component'");
        verify(newStreamStatusRepository).upsertLatestKnownPosition(
                eq(streamId_1),
                eq(source),
                eq(component),
                eq(positionInStream_1),
                eq(now));
        verify(logger).debug("Updating latest known position to '111' for stream id '1111ef5f-2b26-42a2-a01c-4591d2911111', source 'some-source' component 'some-component'");
        verify(newStreamStatusRepository).upsertLatestKnownPosition(
                eq(streamId_2),
                eq(source),
                eq(component),
                eq(positionInStream_2),
                eq(now));
        verify(logger).debug("Updating latest known position to '222' for stream id '2222ef5f-2b26-42a2-a01c-4591d2912222', source 'some-source' component 'some-component'");

        verify(eventSubscriptionStatusRepository).save(new EventSubscriptionStatus(
                source,
                component,
                of(newLatestEventId),
                now
        ));
    }

    @Test
    public void shouldDoNothingNoEventSubscriptionStatusFoundForSourceComponent() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final SourceComponentPair sourceComponentPair = new SourceComponentPair(
                source,
                component);

        when(eventSubscriptionStatusRepository.findBy(source, component)).thenReturn(empty());

        eventDiscoveryWorker.runEventDiscoveryForSourceComponentPair(sourceComponentPair);

        verifyNoInteractions(eventSubscriptionDiscoveryBean);
    }
}