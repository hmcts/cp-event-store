package uk.gov.justice.eventsourcing.discovery.timers;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.eventsourcing.discovery.timers.EventDiscoveryTimerBean.TIMER_JOB_NAME;

import uk.gov.justice.eventsourcing.discovery.workers.EventDiscoveryWorker;
import uk.gov.justice.services.ejb.timer.TimerServiceManager;

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
    private TimerServiceManager timerServiceManager;

    @Mock
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Mock
    private EventDiscoveryWorker eventDiscoveryWorker;

    @InjectMocks
    private EventDiscoveryTimerBean eventDiscoveryTimerBean;

    @Test
    public void shouldStartTheTimerServicePostConstruct() throws Exception {

        final long timerStartWaitMilliseconds = 213L;
        final long timerIntervalMilliseconds = 9872397L;

        when(eventDiscoveryTimerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(eventDiscoveryTimerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        eventDiscoveryTimerBean.startTimerService();

        verify(timerServiceManager).createIntervalTimer(
                TIMER_JOB_NAME,
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerService);
    }

    @Test
    public void shouldRunEventDiscoveryOnTimeout() throws Exception {

        eventDiscoveryTimerBean.runEventDiscovery();

        verify(eventDiscoveryWorker).runEventDiscovery();
    }
}