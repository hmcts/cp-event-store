package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventLinkingTimerBean.TIMER_JOB_NAME;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;

import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkingTimerBeanTest {

    @Mock
    private TimerService timerService;

    @Mock
    private EventPublishingWorkerTimerConfig eventPublishingWorkerTimerConfig;

    @Mock
    private TimerServiceManager timerServiceManager;

    @Mock
    private EventLinkingWorker eventLinkingWorker;

    @InjectMocks
    private EventLinkingTimerBean eventLinkingTimerBean;

    @Test
    public void shouldStartEventLinkingWorker() throws Exception {
        final long timerStartWaitMilliseconds = 23;
        final long timerIntervalMilliseconds = 76;

        when(eventPublishingWorkerTimerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(eventPublishingWorkerTimerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        eventLinkingTimerBean.startTimerService();

        verify(timerServiceManager).createIntervalTimer(
                TIMER_JOB_NAME,
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerService);
    }

    @Test
    public void shouldName() throws Exception {

        eventLinkingTimerBean.runEventLinkingWorker();

        verify(eventLinkingWorker).findAndLinkUnlinkedEvents();
    }
}