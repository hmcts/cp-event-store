package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.util.List;
import java.util.function.Supplier;
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
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;
import uk.gov.justice.services.event.sourcing.subscription.manager.StreamProcessingSubscriptionManager;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculator;
import uk.gov.justice.services.eventsourcing.util.jee.timer.SufficientTimeRemainingCalculatorFactory;
import uk.gov.justice.subscription.SourceComponentPair;
import uk.gov.justice.subscription.SubscriptionSourceComponentFinder;

@Singleton
@Startup
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class StreamProcessingTimerBean {

    @Inject
    private Logger logger;

    @Resource
    private TimerService timerService;

    @Inject
    private StreamProcessingTimerConfig streamProcessingTimerConfig;

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

            sourceComponentPairs.forEach(sourceComponentPair -> {
                final TimerConfig timerConfig = new TimerConfig();
                timerConfig.setPersistent(false);
                timerConfig.setInfo(sourceComponentPair);

                timerService.createIntervalTimer(
                        streamProcessingTimerConfig.getTimerStartWaitMilliseconds(),
                        streamProcessingTimerConfig.getTimerIntervalMilliseconds(),
                        timerConfig
                );
            });
        }
    }

    @Timeout
    public void processStreamEvents(final Timer timer) {
        final SourceComponentPair pair = (SourceComponentPair) timer.getInfo();
        final SufficientTimeRemainingCalculator sufficientTimeRemainingCalculator = sufficientTimeRemainingCalculatorFactory.createNew(timer, streamProcessingTimerConfig.getTimeBetweenRunsMilliseconds());
        try {
            streamProcessingSubscriptionManager.process(pair.source(), pair.component(), sufficientTimeRemainingCalculator);
        } catch (final Exception e) {
            logger.error("Failed to process stream events of source: {}, component: {}", pair.source(), pair.component(), e);
        }
    }
}
