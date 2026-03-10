package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

@ExtendWith(MockitoExtension.class)
public class StreamProcessingConfigTest {

    @InjectMocks
    private StreamProcessingConfig streamProcessingConfig;

    @Test
    public void shouldGetTheStartWaitTime() {

        final long milliseconds = 982374L;

        setField(streamProcessingConfig, "timerStartWaitMilliseconds", "" + milliseconds);

        assertThat(streamProcessingConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() {

        final long milliseconds = 2998734L;

        setField(streamProcessingConfig, "timerIntervalMilliseconds", "" + milliseconds);

        assertThat(streamProcessingConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheMaxWorkers() {

        final int maxWorkers = 5;

        setField(streamProcessingConfig, "maxWorkers", "" + maxWorkers);

        assertThat(streamProcessingConfig.getMaxWorkers(), is(maxWorkers));
    }

    @Test
    public void shouldParseMaxRetriesStringAndReturnAsInteger() throws Exception {

        final Integer maxRetries = 23;
        setField(streamProcessingConfig, "maxRetries", maxRetries + "");

        assertThat(streamProcessingConfig.getMaxRetries(), is(maxRetries));
    }

    @Test
    public void shouldParseRetryDelayStringAndReturnAsLong() throws Exception {

        final Long retryDelay = 75L;
        setField(streamProcessingConfig, "retryDelayMilliseconds", retryDelay + "");

        assertThat(streamProcessingConfig.getRetryDelayMilliseconds(), is(retryDelay));
    }

    @Test
    public void shouldParseRetryDelayMultiplierStringAndReturnAsBigDecimal() throws Exception {

        final BigDecimal retryDelayMultiplier = new BigDecimal("1.23");
        setField(streamProcessingConfig, "retryDelayMultiplier", retryDelayMultiplier + "");

        assertThat(streamProcessingConfig.getRetryDelayMultiplier(), is(retryDelayMultiplier));
    }

    @Test
    public void shouldReturnTrueWhenAccessEventStoreViaRestIsTrue() {

        setField(streamProcessingConfig, "accessEventStoreViaRest", "true");

        assertThat(streamProcessingConfig.accessEventStoreViaRest(), is(true));
    }

    @Test
    public void shouldReturnFalseWhenAccessEventStoreViaRestIsFalse() {

        setField(streamProcessingConfig, "accessEventStoreViaRest", "false");

        assertThat(streamProcessingConfig.accessEventStoreViaRest(), is(false));
    }

    @Test
    public void shouldGetTheIdleThreshold() {

        final long milliseconds = 2000L;

        setField(streamProcessingConfig, "idleThresholdMilliseconds", "" + milliseconds);

        assertThat(streamProcessingConfig.getIdleThresholdMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldParseCircuitBreakerFailureThresholdAndReturnAsInt() {

        final int threshold = 5;
        setField(streamProcessingConfig, "circuitBreakerFailureThreshold", threshold + "");

        assertThat(streamProcessingConfig.getCircuitBreakerFailureThreshold(), is(threshold));
    }

    @Test
    public void shouldParseCircuitBreakerCooldownMillisecondsAndReturnAsLong() {

        final long cooldown = 30000L;
        setField(streamProcessingConfig, "circuitBreakerCoolDownMilliseconds", cooldown + "");

        assertThat(streamProcessingConfig.getCircuitBreakerCoolDownMilliseconds(), is(cooldown));
    }
}
