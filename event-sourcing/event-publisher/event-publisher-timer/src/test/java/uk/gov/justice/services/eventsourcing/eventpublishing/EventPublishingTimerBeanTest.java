package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventPublishingTimerBean.TIMER_JOB_NAME;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;

import javax.ejb.Timer;
import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory;

@ExtendWith(MockitoExtension.class)
public class EventPublishingTimerBeanTest {

    @Mock
    private TimerService timerService;

    @Mock
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Mock
    private TimerServiceManager timerServiceManager;

    @Mock
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @Mock
    private EventPublishingWorker eventPublishingWorker;

    @InjectMocks
    private EventPublishingTimerBean eventPublishingTimerBean;

    @Test
    public void shouldStartEventLinkingWorkerAndSetTimer() throws Exception {
        final long timerStartWaitMilliseconds = 23;
        final long timerIntervalMilliseconds = 76;

        when(eventPublishingWorkerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(eventPublishingWorkerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        eventPublishingTimerBean.startTimerService();

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

        when(eventPublishingWorkerConfig.getTimeBetweenRunsMilliseconds()).thenReturn(timeBetweenRunsMilliseconds);
        when(sufficientTimeRemainingCalculatorFactory.createNew(
                timer,
                timeBetweenRunsMilliseconds)).thenReturn(sufficientTimeRemainingCalculator);

        eventPublishingTimerBean.runEventPublishing(timer);

        verify(eventPublishingWorker).publishNewEvents(sufficientTimeRemainingCalculator);
    }
}