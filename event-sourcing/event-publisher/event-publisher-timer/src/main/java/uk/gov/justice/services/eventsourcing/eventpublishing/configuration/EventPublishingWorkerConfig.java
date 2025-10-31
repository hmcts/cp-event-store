package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EventPublishingWorkerConfig {

    @Inject
    @Value(key = "event.publishing.worker.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.time.between.runs.milliseconds", defaultValue = "5")
    private String timeBetweenRunsMilliseconds;


    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimeBetweenRunsMilliseconds() {
        return parseLong(timeBetweenRunsMilliseconds);
    }


}
