package uk.gov.justice.eventsourcing.discovery.timers;

import static jakarta.ejb.TransactionAttributeType.NEVER;

import uk.gov.justice.eventsourcing.discovery.workers.EventDiscoveryWorker;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.ejb.timer.TimerConfigFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.ConcurrencyManagement;
import jakarta.ejb.ConcurrencyManagementType;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.ejb.Timeout;
import jakarta.ejb.Timer;
import jakarta.ejb.TimerConfig;
import jakarta.ejb.TimerService;
import jakarta.ejb.TransactionAttribute;
import jakarta.inject.Inject;

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
