package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventLinkingTimerBean.TIMER_JOB_NAME;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;

import javax.ejb.Timer;
import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;

@ExtendWith(MockitoExtension.class)
public class EventLinkingTimerBeanTest {

    @Mock
    private TimerService timerService;

    @Mock
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Mock
    private TimerServiceManager timerServiceManager;

    @Mock
    private uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @Mock
    private EventLinkingWorker eventLinkingWorker;

    @InjectMocks
    private EventLinkingTimerBean eventLinkingTimerBean;

    @Test
    public void shouldStartEventLinkingWorkerAndSetTimer() throws Exception {
        final long timerStartWaitMilliseconds = 23;
        final long timerIntervalMilliseconds = 76;

        when(eventLinkingWorkerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(eventLinkingWorkerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        eventLinkingTimerBean.startTimerService();

        verify(timerServiceManager).createIntervalTimer(
                TIMER_JOB_NAME,
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerService);
    }

    @Test
    public void shouldRunEventLinkingWorker() throws Exception {

        final long timeBetweenRunsMilliseconds = 23L;

        final Timer timer = mock(Timer.class);
        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = mock(SufficientTimeRemainingCalculator.class);

        when(eventLinkingWorkerConfig.getTimeBetweenRunsMilliseconds()).thenReturn(timeBetweenRunsMilliseconds);
        when(sufficientTimeRemainingCalculatorFactory.createNew(
                timer,
                timeBetweenRunsMilliseconds)).thenReturn(sufficientTimeRemainingCalculator);

        eventLinkingTimerBean.runEventLinkingWorker(timer);

        verify(eventLinkingWorker).linkNewEvents(sufficientTimeRemainingCalculator);
    }
}