package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;

public class EventLinkingWorkerConfig {

    @Inject
    @Value(key = "event.linking.worker.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.timer.interval.milliseconds", defaultValue = "500")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.time.between.runs.milliseconds", defaultValue = "100")
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
