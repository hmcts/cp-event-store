package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.GlobalValue;
import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.eventsourcing.publishedevent.NotifierWorkerConfig;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class PrePublisherTimerConfig extends NotifierWorkerConfig {

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
    @Value(key = "event.linking.worker.notified", defaultValue = "false")
    private String eventLinkerNotified;

    @Inject
    @Value(key = "event.linking.worker.backoff.min.milliseconds", defaultValue = "5")
    private String backoffMinMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.backoff.max.milliseconds", defaultValue = "100")
    private String backoffMaxMilliseconds;

    @Inject
    @Value(key = "event.linking.worker.backoff.multiplier", defaultValue = "1.5")
    private String backoffMultiplier;

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

    @PostConstruct
    public void postConstruct() {
        this.setShouldWorkerNotified(parseBoolean(eventLinkerNotified));
        this.setBackoffMinMilliseconds(parseLong(backoffMinMilliseconds));
        this.setBackoffMaxMilliseconds(parseLong(backoffMaxMilliseconds));
        this.setBackoffMultiplier(parseDouble(backoffMultiplier));
    }
}
