package uk.gov.justice.eventsourcing.discovery.timers;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;
import uk.gov.justice.services.common.configuration.subscription.pull.EventPullConfiguration;

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

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }

    public boolean shouldDiscoveryNotified() {
        return eventPullConfiguration.shouldProcessEventsByPullMechanism() && parseBoolean(discoveryNotified);
    }
}
