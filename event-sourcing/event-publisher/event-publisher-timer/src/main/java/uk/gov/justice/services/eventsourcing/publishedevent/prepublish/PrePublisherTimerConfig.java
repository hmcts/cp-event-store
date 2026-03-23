package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;
import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;

public class PrePublisherTimerConfig {

    @Inject
    @GlobalValue(key = "pre.publish.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @GlobalValue(key = "pre.publish.timer.interval.milliseconds", defaultValue = "500")
    private String timerIntervalMilliseconds;

    @Inject
    @GlobalValue(key = "pre.publish.timer.max.runtime.milliseconds", defaultValue = "450")
    private String timerMaxRuntimeMilliseconds;

    @Inject
    @GlobalValue(key = "pre.publish.disable", defaultValue = "false")
    private String disablePrePublish;

    @Inject
    @Value(key = "pre.publish.worker.notified", defaultValue = "false")
    private String eventLinkerNotified;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimerMaxRuntimeMilliseconds() {
        return parseLong(timerMaxRuntimeMilliseconds);
    }

    public boolean isDisabled() {
        return parseBoolean(disablePrePublish);
    }

    public void setDisabled(final boolean disable) {
        this.disablePrePublish = Boolean.toString(disable);
    }

    public boolean shouldWorkerNotified() {
        return parseBoolean(eventLinkerNotified);
    }
}
