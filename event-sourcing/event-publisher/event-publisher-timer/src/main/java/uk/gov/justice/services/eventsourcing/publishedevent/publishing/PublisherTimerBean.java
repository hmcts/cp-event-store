package uk.gov.justice.services.eventsourcing.publishedevent.publishing;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.TimerService;
import javax.inject.Inject;

@Singleton
@Startup
public class PublisherTimerBean {

    private static final String TIMER_JOB_NAME = "event-store.de-queue-events-and-publish.job";

    @Resource
    private TimerService timerService;

    @Inject
    private PublisherTimerConfig publisherTimerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private AsynchronousPublisher asynchronousPublisher;

    @Inject
    private EventPublishingNotifier eventPublishingNotifier;

    @PostConstruct
    public void startTimerService() {

        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                publisherTimerConfig.getTimerStartWaitMilliseconds(),
                publisherTimerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void doDeQueueAndPublish() {

        if (!publisherTimerConfig.isDisabled()) {
            if (publisherTimerConfig.shouldWorkerNotified()) {
                eventPublishingNotifier.wakeUp(true);
            } else {
                asynchronousPublisher.doDeQueueAndPublish();
            }
        }
    }
}
