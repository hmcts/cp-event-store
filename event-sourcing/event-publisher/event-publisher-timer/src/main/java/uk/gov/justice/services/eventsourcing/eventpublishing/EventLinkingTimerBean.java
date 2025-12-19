package uk.gov.justice.services.eventsourcing.eventpublishing;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerService;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;

@Singleton
@Startup
@TransactionAttribute(TransactionAttributeType.NEVER)
public class EventLinkingTimerBean {

    static final String TIMER_JOB_NAME = "event-store.link-new-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private EventLinkingWorker eventLinkingWorker;

    @Inject
    private EventLinkingNotifier eventLinkingNotifier;

    @Inject
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @PostConstruct
    public void startTimerService() {
        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                eventLinkingWorkerConfig.getTimerStartWaitMilliseconds(),
                eventLinkingWorkerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventLinkingWorker(final Timer timer) {
        if (eventLinkingWorkerConfig.shouldWorkerNotified())
            eventLinkingNotifier.wakeUp(true);
        else {
            eventLinkingWorker.linkNewEvents(sufficientTimeRemainingCalculatorFactory.createNew(
                    timer,
                    eventLinkingWorkerConfig.getTimeBetweenRunsMilliseconds()));
        }
    }
}
