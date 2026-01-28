package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import static java.lang.Long.parseLong;

import uk.gov.justice.services.common.configuration.Value;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StreamProcessingTimerConfig {

    @Inject
    @Value(key = "stream.processing.timer.start.wait.milliseconds", defaultValue = "7250")
    private String timerStartWaitMilliseconds;

    @Inject
    @Value(key = "stream.processing.timer.interval.milliseconds", defaultValue = "100")
    private String timerIntervalMilliseconds;

    public long getTimerStartWaitMilliseconds() {
        return parseLong(timerStartWaitMilliseconds);
    }

    public long getTimerIntervalMilliseconds() {
        return parseLong(timerIntervalMilliseconds);
    }
}
