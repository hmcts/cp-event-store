package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerBean;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerFactory;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.StreamProcessingWorkerTask;
import uk.gov.justice.services.event.sourcing.subscription.manager.task.WorkerActivityTracker;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.List;

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
import javax.ejb.TransactionAttributeType;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class StreamProcessingCoordinator {

    @Inject
    private Logger logger;

    @Resource
    private TimerService timerService;

    @Resource
    private ManagedExecutorService managedExecutorService;

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Inject
    private StreamProcessingWorkerBean streamProcessingWorkerBean;

    @Inject
    private StreamProcessingWorkerFactory streamProcessingWorkerFactory;

    @Inject
    private WorkerActivityTracker workerActivityTracker;

    @PostConstruct
    public void startTimerService() {
        if (eventPullConfiguration.shouldProcessEventsByPullMechanism()) {
            final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                    .findListenerOrIndexerPairs();

            sourceComponentPairs.forEach(this::createCoordinatorTimer);
        }
    }

    private void createCoordinatorTimer(final SourceComponentPair sourceComponentPair) {
        final TimerConfig timerConfig = new TimerConfig();
        timerConfig.setPersistent(false);
        timerConfig.setInfo(sourceComponentPair);

        timerService.createIntervalTimer(
                streamProcessingConfig.getTimerStartWaitMilliseconds(),
                streamProcessingConfig.getTimerIntervalMilliseconds(),
                timerConfig
        );
    }

    @Timeout
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void coordinateWorkers(final Timer timer) {
        final SourceComponentPair pair = (SourceComponentPair) timer.getInfo();

        try {
            final int activeCount = workerActivityTracker.getActiveCount(pair);
            final boolean recentlyActive = workerActivityTracker.resetRecentlyActive(pair);
            final int maxWorkers = streamProcessingConfig.getMaxWorkers();

            if (recentlyActive) {
                final int toSpawn = maxWorkers - activeCount;
                spawnWorkers(pair, toSpawn);
            } else if (activeCount == 0) {
                final boolean foundEvents = streamProcessingWorkerBean.processUntilIdle(
                        pair.source(), pair.component());
                if (foundEvents) {
                    workerActivityTracker.markRecentlyActive(pair);
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to coordinate workers for source: {}, component: {}",
                    pair.source(), pair.component(), e);
        }
    }

    private void spawnWorkers(final SourceComponentPair pair, final int count) {
        for (int i = 0; i < count; i++) {
            final StreamProcessingWorkerTask task = streamProcessingWorkerFactory.createWorkerTask(pair);
            managedExecutorService.execute(task);
        }
    }
}
