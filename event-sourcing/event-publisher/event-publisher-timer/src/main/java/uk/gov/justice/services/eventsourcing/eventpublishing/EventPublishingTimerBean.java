package uk.gov.justice.services.eventsourcing.eventpublishing;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerService;
import javax.inject.Inject;

@Singleton
@Startup
public class EventPublishingTimerBean {

    static final String TIMER_JOB_NAME = "event-store.publish-new-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private EventPublishingWorker eventPublishingWorker;

    @Inject
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @PostConstruct
    public void startTimerService() {

        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                eventPublishingWorkerConfig.getTimerStartWaitMilliseconds(),
                eventPublishingWorkerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventPublishing(final Timer timer) {

        final long timeBetweenRunsMilliseconds = eventPublishingWorkerConfig.getTimeBetweenRunsMilliseconds();

        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = sufficientTimeRemainingCalculatorFactory.createNew(
                timer,
                timeBetweenRunsMilliseconds);


        eventPublishingWorker.publishNewEvents(sufficientTimeRemainingCalculator);
    }
}
