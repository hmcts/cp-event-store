package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.publishedevent.publishing.PublisherTimerConfig;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.TimerService;
import javax.inject.Inject;

@Singleton
@Startup
public class EventPublishingTimerBean {

    static final String TIMER_JOB_NAME = "event-store.publish-queued-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private PublisherTimerConfig publisherTimerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private LinkedEventPublishingWorker linkedEventPublishingWorker;

    @PostConstruct
    public void startTimerService() {

        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                publisherTimerConfig.getTimerStartWaitMilliseconds(),
                publisherTimerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventPublishing() {
        linkedEventPublishingWorker.publishQueuedEvents();
    }
}
