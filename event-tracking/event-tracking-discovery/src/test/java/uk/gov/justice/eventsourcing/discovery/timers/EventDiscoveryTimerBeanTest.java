package uk.gov.justice.eventsourcing.discovery.timers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.eventsourcing.discovery.workers.EventDiscoveryWorker;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.ejb.timer.TimerConfigFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.List;

import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryTimerBeanTest {

    @Mock
    private TimerService timerService;

    @Mock
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Mock
    private EventPullConfiguration eventPullConfiguration;

    @Mock
    private EventDiscoveryWorker eventDiscoveryWorker;

    @Mock
    private TimerConfigFactory timerConfigFactory;

    @Mock
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @InjectMocks
    private EventDiscoveryTimerBean eventDiscoveryTimerBean;

    @Test
    public void shouldStartTheTimerServicePostConstructForAllSourceComponentPairs() throws Exception {

        final long timerStartWaitMilliseconds = 213L;
        final long timerIntervalMilliseconds = 9872397L;

        final SourceComponentPair sourceComponentPair_1 = mock(SourceComponentPair.class);
        final SourceComponentPair sourceComponentPair_2 = mock(SourceComponentPair.class);

        final TimerConfig timerConfig_1 = mock(TimerConfig.class);
        final TimerConfig timerConfig_2 = mock(TimerConfig.class);

        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(true);
        when(timerConfigFactory.createNew()).thenReturn(timerConfig_1, timerConfig_2);
        when(eventDiscoveryTimerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(eventDiscoveryTimerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);
        when(subscriptionSourceComponentFinder.findListenerOrIndexerPairs()).thenReturn(List.of(sourceComponentPair_1, sourceComponentPair_2));

        eventDiscoveryTimerBean.startTimerService();

        verify(timerConfig_1).setPersistent(false);
        verify(timerConfig_1).setInfo(sourceComponentPair_1);
        verify(timerService).createIntervalTimer(
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerConfig_1);

        verify(timerConfig_2).setPersistent(false);
        verify(timerConfig_2).setInfo(sourceComponentPair_2);
        verify(timerService).createIntervalTimer(
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerConfig_2);
    }

    @Test
    public void shouldNotStartTimerIfEventPullMechanismNotEnabled() throws Exception {

        when(eventPullConfiguration.shouldProcessEventsByPullMechanism()).thenReturn(false);

        eventDiscoveryTimerBean.startTimerService();
        
        verifyNoInteractions(subscriptionSourceComponentFinder);
        verifyNoInteractions(timerConfigFactory);
        verifyNoInteractions(timerService);
    }

    @Test
    public void shouldRunEventDiscoveryOnTimeout() throws Exception {

        final Timer timer = mock(Timer.class);
        final SourceComponentPair sourceComponentPair = mock(SourceComponentPair.class);

        when(timer.getInfo()).thenReturn(sourceComponentPair);

        eventDiscoveryTimerBean.runEventDiscovery(timer);

        verify(eventDiscoveryWorker).runEventDiscoveryForSourceComponentPair(sourceComponentPair);
    }
}