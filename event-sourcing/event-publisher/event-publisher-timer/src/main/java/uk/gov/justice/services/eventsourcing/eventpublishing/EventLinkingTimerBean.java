package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Timeout;
import javax.ejb.TimerService;
import javax.inject.Inject;

public class EventLinkingTimerBean {

    static final String TIMER_JOB_NAME = "event-store.link-events.worker";

    @Resource
    private TimerService timerService;

    @Inject
    private EventPublishingWorkerTimerConfig eventPublishingWorkerTimerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private EventLinkingWorker eventLinkingWorker;

    @PostConstruct
    public void startTimerService() {

        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                eventPublishingWorkerTimerConfig.getTimerStartWaitMilliseconds(),
                eventPublishingWorkerTimerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventLinkingWorker() {
        eventLinkingWorker.findAndLinkUnlinkedEvents();
    }
}
