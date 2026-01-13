package uk.gov.justice.eventsourcing.discovery.timers;

import static javax.ejb.TransactionAttributeType.NEVER;

import uk.gov.justice.eventsourcing.discovery.workers.EventDiscoveryWorker;
import uk.gov.justice.services.ejb.timer.TimerServiceManager;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.TimerService;
import javax.ejb.TransactionAttribute;
import javax.inject.Inject;

@Singleton
@Startup
@TransactionAttribute(NEVER)
public class EventDiscoveryTimerBean {

    static final String TIMER_JOB_NAME = "view-store.discover-new-events.job";

    @Resource
    private TimerService timerService;

    @Inject
    private TimerServiceManager timerServiceManager;

    @Inject
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Inject
    private EventDiscoveryWorker eventDiscoveryWorker;

    @PostConstruct
    public void startTimerService() {
        timerServiceManager.createIntervalTimer(
                TIMER_JOB_NAME,
                eventDiscoveryTimerConfig.getTimerStartWaitMilliseconds(),
                eventDiscoveryTimerConfig.getTimerIntervalMilliseconds(),
                timerService);
    }

    @Timeout
    public void runEventDiscovery() {
        eventDiscoveryWorker.runEventDiscovery();
    }
}
