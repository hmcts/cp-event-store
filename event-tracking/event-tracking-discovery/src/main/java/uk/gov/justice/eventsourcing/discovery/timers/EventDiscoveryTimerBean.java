package uk.gov.justice.eventsourcing.discovery.timers;

import static javax.ejb.TransactionAttributeType.NEVER;

import uk.gov.justice.eventsourcing.discovery.workers.EventDiscoveryWorker;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.ejb.timer.TimerConfigFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.Timer;
import javax.ejb.TimerConfig;
import javax.ejb.TimerService;
import javax.ejb.TransactionAttribute;
import javax.inject.Inject;

@Singleton
@Startup
@TransactionAttribute(NEVER)
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class EventDiscoveryTimerBean {

    @Resource
    private TimerService timerService;

    @Inject
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Inject
    private EventDiscoveryWorker eventDiscoveryWorker;

    @Inject
    private TimerConfigFactory timerConfigFactory;

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @PostConstruct
    public void startTimerService() {
        if (eventPullConfiguration.shouldProcessEventsByPullMechanism()) {
            subscriptionSourceComponentFinder
                    .findListenerOrIndexerPairs()
                    .forEach(this::startSourceComponentPairTimer);
        }
    }

    @Timeout
    public void runEventDiscovery(final Timer timer) {
        eventDiscoveryWorker.runEventDiscoveryForSourceComponentPair((SourceComponentPair) timer.getInfo());
    }

    private void startSourceComponentPairTimer(final SourceComponentPair sourceComponentPair) {
        final TimerConfig timerConfig = timerConfigFactory.createNew();
        timerConfig.setPersistent(false);
        timerConfig.setInfo(sourceComponentPair);

        timerService.createIntervalTimer(
                eventDiscoveryTimerConfig.getTimerStartWaitMilliseconds(),
                eventDiscoveryTimerConfig.getTimerIntervalMilliseconds(),
                timerConfig);
    }

}
