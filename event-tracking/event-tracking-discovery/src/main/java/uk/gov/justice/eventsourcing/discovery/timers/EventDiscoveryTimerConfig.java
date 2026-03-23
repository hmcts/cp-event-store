package uk.gov.justice.eventsourcing.discovery.timers;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class EventDiscoveryTimerConfig {

    @Inject
    private EventPullConfiguration eventPullConfiguration;

    @Inject
    @Value(key = "event.discovery.timer.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "event.discovery.timer.interval.milliseconds", defaultValue = "1000")
    private String timerIntervalMilliseconds;

    @Inject
    @Value(key = "event.discovery.notified", defaultValue = "false")
    private String discoveryNotified;

    @Inject
    @Value(key = "event.discovery.backoff.min.milliseconds", defaultValue = "5")
    private String backoffMinMilliseconds;

    @Inject
    @Value(key = "event.discovery.backoff.max.milliseconds", defaultValue = "1000")
    private String backoffMaxMilliseconds;

    @Inject
    @Value(key = "event.discovery.backoff.multiplier", defaultValue = "1.5")
    private String backoffMultiplier;

    private boolean shouldDiscoveryNotified;
    private long backoffMin;
    private long backoffMax;
    private double backoffMult;

    @PostConstruct
    public void postConstruct() {
        this.shouldDiscoveryNotified = parseBoolean(discoveryNotified);
        this.backoffMin = parseLong(backoffMinMilliseconds);
        this.backoffMax = parseLong(backoffMaxMilliseconds);
        this.backoffMult = parseDouble(backoffMultiplier);
    }

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public boolean shouldDiscoveryNotified() {
        return eventPullConfiguration.shouldProcessEventsByPullMechanism() && shouldDiscoveryNotified;
    }

    public long getBackoffMinMilliseconds() {
        return backoffMin;
    }

    public long getBackoffMaxMilliseconds() {
        return backoffMax;
    }

    public double getBackoffMultiplier() {
        return backoffMult;
    }
}
