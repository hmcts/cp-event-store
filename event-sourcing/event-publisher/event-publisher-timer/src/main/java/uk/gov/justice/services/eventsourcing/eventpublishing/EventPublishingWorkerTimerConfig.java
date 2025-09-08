package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;

import javax.inject.Inject;

public class EventPublishingWorkerTimerConfig {

    @Inject
    @GlobalValue(key = "event.publishing.worker.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @GlobalValue(key = "event.publishing.worker.timer.interval.milliseconds", defaultValue = "500")
    private String timerIntervalMilliseconds;

    @Inject
    @GlobalValue(key = "event.publishing.worker.max.runtime.milliseconds", defaultValue = "450")
    private String timerMaxRuntimeMilliseconds;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimerMaxRuntimeMilliseconds() {
        return parseLong(timerMaxRuntimeMilliseconds);
    }
}
