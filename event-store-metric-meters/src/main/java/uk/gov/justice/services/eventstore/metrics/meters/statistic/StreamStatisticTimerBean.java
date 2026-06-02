package uk.gov.justice.services.eventstore.metrics.meters.statistic;

import uk.gov.justice.services.ejb.timer.TimerServiceManager;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;
import uk.gov.justice.services.metrics.micrometer.config.MetricsConfiguration;

import java.sql.Timestamp;
import java.time.Instant;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.ejb.Timeout;
import jakarta.ejb.TimerService;
import jakarta.inject.Inject;

import org.slf4j.Logger;

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
