package uk.gov.justice.services.eventsourcing.eventpublishing;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventPublishingTimerBean.TIMER_JOB_NAME;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;

import javax.ejb.TimerService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventPublishingTimerBeanTest {

    @Mock
    private TimerService timerService;

    @Mock
    private PublisherTimerConfig publisherTimerConfig;

    @Mock
    private TimerServiceManager timerServiceManager;

    @Mock
    private LinkedEventPublishingWorker linkedEventPublishingWorker;

    @InjectMocks
    private EventPublishingTimerBean eventPublishingTimerBean;

    @Test
    public void shouldStartTheTimeOnPostConstructWithTheCorrectConfiguration() throws Exception {

        final long timerStartWaitMilliseconds = 239847L;
        final long timerIntervalMilliseconds = 92837L;

        when(publisherTimerConfig.getTimerStartWaitMilliseconds()).thenReturn(timerStartWaitMilliseconds);
        when(publisherTimerConfig.getTimerIntervalMilliseconds()).thenReturn(timerIntervalMilliseconds);

        eventPublishingTimerBean.startTimerService();

        verify(timerServiceManager).createIntervalTimer(
                TIMER_JOB_NAME,
                timerStartWaitMilliseconds,
                timerIntervalMilliseconds,
                timerService);
    }

    @Test
    public void shouldRunTheEventPublishingWorkerOnTimeout() throws Exception {

        eventPublishingTimerBean.runEventPublishing();

        verify(linkedEventPublishingWorker).publishQueuedEvents();
    }
}