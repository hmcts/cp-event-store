package uk.gov.justice.services.eventstore.metrics.meters.statistic;

import org.slf4j.Logger;
import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;

import java.sql.Timestamp;
import java.time.Instant;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.Timeout;
import javax.ejb.TimerService;
import javax.inject.Inject;

@Singleton
@Startup
public class StreamStatisticTimerBean {

    @Inject
    private Logger logger;

    private static final String TIMER_JOB_NAME = "event-store.stream-statistic-refresh.job";

    @Inject
    private TimerServiceManager timerServiceManager;

    @Resource
    private TimerService timerService;

    @Inject
    private MetricsConfiguration metricsConfiguration;

    @Inject
    private StreamMetricsRepository streamMetricsRepository;

    @PostConstruct
    public void startTimerService() {
        if (metricsConfiguration.micrometerMetricsEnabled()) {
            timerServiceManager.createIntervalTimer(
                    TIMER_JOB_NAME,
                    metricsConfiguration.statisticTimerDelayMilliseconds(),
                    metricsConfiguration.statisticTimerIntervalMilliseconds(),
                    timerService);
        }
    }

    @Timeout
    public void calculateStreamStatistic() {
        final long timerIntervalMillis = metricsConfiguration.statisticTimerIntervalMilliseconds();
        final Timestamp freshnessLimit = Timestamp.from(Instant.now().minusMillis(timerIntervalMillis));
        try {
            streamMetricsRepository.calculateStreamStatistic(freshnessLimit);
        } catch (Exception e) {
            logger.warn("Error calculating stream statistic", e);
        }
    }
}
