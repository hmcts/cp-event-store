package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class EventPublishingWorkerConfig extends NotifierWorkerConfig {

    @Inject
    @Value(key = "event.publishing.worker.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.time.between.runs.milliseconds", defaultValue = "5")
    private String timeBetweenRunsMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.notified", defaultValue = "false")
    private String eventPublisherNotified;

    @Inject
    @Value(key = "event.publishing.worker.backoff.min.milliseconds", defaultValue = "5")
    private String backoffMinMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.backoff.max.milliseconds", defaultValue = "100")
    private String backoffMaxMilliseconds;

    @Inject
    @Value(key = "event.publishing.worker.backoff.multiplier", defaultValue = "1.5")
    private String backoffMultiplier;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public long getTimeBetweenRunsMilliseconds() {
        return parseLong(timeBetweenRunsMilliseconds);
    }


    @PostConstruct
    public void postConstruct() {
        this.setShouldWorkerNotified(parseBoolean(eventPublisherNotified));
        this.setBackoffMinMilliseconds(parseLong(backoffMinMilliseconds));
        this.setBackoffMaxMilliseconds(parseLong(backoffMaxMilliseconds));
        this.setBackoffMultiplier(parseDouble(backoffMultiplier));
    }

}
