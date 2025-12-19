package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

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
public class PrePublisherTimerBean {

    private static final String TIMER_JOB_NAME = "event-store.pre-publish-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private PrePublisherTimerConfig prePublisherTimerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private AsynchronousPrePublisher asynchronousPrePublisher;

    @Inject
    private EventLinkingNotifier eventLinkingNotifier;

    @PostConstruct
    public void startTimerService() {

        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                prePublisherTimerConfig.getTimerStartWaitMilliseconds(),
                prePublisherTimerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void performPrePublish() {

        if (! prePublisherTimerConfig.isDisabled()) {
            if (prePublisherTimerConfig.shouldWorkerNotified())
                eventLinkingNotifier.wakeUp(true);
            else {
                asynchronousPrePublisher.performPrePublish();
            }

        }
    }
}
