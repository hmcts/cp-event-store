package uk.gov.justice.services.eventsourcing.publishedevent.publishing;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;
import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class PublisherTimerConfig {

    @Inject
    @GlobalValue(key = "event.dequer.start.wait.milliseconds", defaultValue = "7000")
    String timerStartWaitMilliseconds;

    @Inject
    @GlobalValue(key = "event.dequer.timer.interval.milliseconds", defaultValue = "500")
    String timerIntervalMilliseconds;

    @Inject
    @GlobalValue(key = "event.dequer.timer.max.runtime.milliseconds", defaultValue = "450")
    String timerMaxRuntimeMilliseconds;

    @Inject
    @GlobalValue(key = "publish.disable", defaultValue = "false")
    private String disablePublish;

    @Inject
    @Value(key = "event.publishing.worker.notified", defaultValue = "false")
    private String eventPublisherNotified;

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
        return parseBoolean(disablePublish);
    }

    public void setDisabled(final boolean disable) {
        this.disablePublish = Boolean.toString(disable);
    }

    public boolean shouldWorkerNotified() {
        return parseBoolean(eventPublisherNotified);
    }
}
