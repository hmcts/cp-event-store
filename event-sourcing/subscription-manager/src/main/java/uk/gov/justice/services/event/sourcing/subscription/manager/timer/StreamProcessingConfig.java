package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StreamProcessingConfig {

    @Inject
    @Value(key = "stream.processing.timer.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "stream.processing.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "stream.processing.timer.between.runs.milliseconds", defaultValue = "5")
    private String timeBetweenRunsMilliseconds;

    @Inject
    @Value(key = "stream.processing.max.threads", defaultValue = "15")
    private String maxThreads;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimeBetweenRunsMilliseconds() {
        return parseLong(timeBetweenRunsMilliseconds);
    }

    public int getMaxThreads() {
        return parseInt(maxThreads);
    }
}
