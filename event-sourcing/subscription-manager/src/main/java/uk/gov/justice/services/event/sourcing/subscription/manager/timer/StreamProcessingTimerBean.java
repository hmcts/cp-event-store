package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamProcessingSubscriptionManager;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import javax.inject.Inject;

import org.slf4j.Logger;

@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class StreamProcessingTimerBean {

    private final ConcurrentHashMap<WorkerTimerInfo, Lock> workerLocks = new ConcurrentHashMap<>();

    @Inject
    private Logger logger;

    @Resource
    private TimerService timerService;

    @Inject
    private StreamProcessingConfig streamProcessingConfig;

    @Inject
    private SubscriptionSourceComponentFinder subscriptionSourceComponentFinder;

    @Inject
    private StreamProcessingSubscriptionManager streamProcessingSubscriptionManager;

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Inject
    private SufficientTimeRemainingCalculatorFactory sufficientTimeRemainingCalculatorFactory;

    @PostConstruct
    public void startTimerService() {
        if (eventPullConfiguration.shouldProcessEventsByPullMechanism()) {
            final List<SourceComponentPair> sourceComponentPairs = subscriptionSourceComponentFinder
                    .findListenerOrIndexerPairs();

            final int maxWorkers = streamProcessingConfig.getMaxWorkers();

            sourceComponentPairs.forEach(sourceComponentPair -> {
                for (int workerNumber = 0; workerNumber < maxWorkers; workerNumber++) {
                    createTimerFor(sourceComponentPair, workerNumber);
                }
            });
        }
    }

    private void createTimerFor(SourceComponentPair sourceComponentPair, int workerNumber) {
        final WorkerTimerInfo workerTimerInfo = new WorkerTimerInfo(sourceComponentPair, workerNumber);
        workerLocks.put(workerTimerInfo, new ReentrantLock());

        final TimerConfig timerConfig = new TimerConfig();
        timerConfig.setPersistent(false);
        timerConfig.setInfo(workerTimerInfo);

        timerService.createIntervalTimer(
                streamProcessingConfig.getTimerStartWaitMilliseconds(),
                streamProcessingConfig.getTimerIntervalMilliseconds(),
                timerConfig
        );
    }

    @Timeout
    public void processStreamEvents(final Timer timer) {
        final WorkerTimerInfo workerTimerInfo = (WorkerTimerInfo) timer.getInfo();
        final SourceComponentPair pair = workerTimerInfo.sourceComponentPair();
        final Lock lock = workerLocks.get(workerTimerInfo);

        if (lock.tryLock()) {
            try {
                final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator =
                        sufficientTimeRemainingCalculatorFactory.createNew(timer, streamProcessingConfig.getTimeBetweenRunsMilliseconds());
                streamProcessingSubscriptionManager.process(pair.source(), pair.component(), sufficientTimeRemainingCalculator);
            } catch (final Exception e) {
                logger.error("Failed to process stream events of source: {}, component: {}, worker: {}",
                        pair.source(), pair.component(), workerTimerInfo.workerNumber(), e);
            } finally {
                lock.unlock();
            }
        } else {
            logger.info("Skipping timer execution for source: {}, component: {}, worker: {} - previous execution still in progress",
                    pair.source(), pair.component(), workerTimerInfo.workerNumber());
        }
    }
}
