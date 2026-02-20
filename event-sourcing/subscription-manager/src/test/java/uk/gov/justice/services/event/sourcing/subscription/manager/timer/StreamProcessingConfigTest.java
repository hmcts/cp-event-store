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
    public void shouldCacheMaxRetriesOnceParsed() throws Exception {

        final Integer maxRetries = 42;

        setField(streamProcessingConfig, "maxRetries", maxRetries + "");
        assertThat(streamProcessingConfig.getMaxRetries(), is(maxRetries));

        setField(streamProcessingConfig, "maxRetries", 66 + "");
        assertThat(streamProcessingConfig.getMaxRetries(), is(maxRetries));

        setField(streamProcessingConfig, "maxRetries", 111 + "");
        assertThat(streamProcessingConfig.getMaxRetries(), is(maxRetries));
    }

    @Test
    public void shouldParseRetryDelayStringAndReturnAsLong() throws Exception {

        final Long retryDelay = 75L;
        setField(streamProcessingConfig, "retryDelayMilliseconds", retryDelay + "");

        assertThat(streamProcessingConfig.getRetryDelayMilliseconds(), is(retryDelay));
    }

    @Test
    public void shouldCacheRetryDelayOnceParsed() throws Exception {

        final Long retryDelay = 32L;

        setField(streamProcessingConfig, "retryDelayMilliseconds", retryDelay + "");
        assertThat(streamProcessingConfig.getRetryDelayMilliseconds(), is(retryDelay));

        setField(streamProcessingConfig, "retryDelayMilliseconds", 55 + "");
        assertThat(streamProcessingConfig.getRetryDelayMilliseconds(), is(retryDelay));

        setField(streamProcessingConfig, "retryDelayMilliseconds", 87 + "");
        assertThat(streamProcessingConfig.getRetryDelayMilliseconds(), is(retryDelay));
    }

    @Test
    public void shouldParseRetryDelayMultiplierStringAndReturnAsBigDecimal() throws Exception {

        final BigDecimal retryDelayMultiplier = new BigDecimal("1.23");
        setField(streamProcessingConfig, "retryDelayMultiplier", retryDelayMultiplier + "");

        assertThat(streamProcessingConfig.getRetryDelayMultiplier(), is(retryDelayMultiplier));
    }

    @Test
    public void shouldCacheRetryDelayMultiplierOnceParsed() throws Exception {

        final BigDecimal retryDelayMultiplier = new BigDecimal("3.142");

        setField(streamProcessingConfig, "retryDelayMultiplier", retryDelayMultiplier + "");
        assertThat(streamProcessingConfig.getRetryDelayMultiplier(), is(retryDelayMultiplier));

        setField(streamProcessingConfig, "retryDelayMultiplier", "5.322");
        assertThat(streamProcessingConfig.getRetryDelayMultiplier(), is(retryDelayMultiplier));

        setField(streamProcessingConfig, "retryDelayMultiplier", "2.887");
        assertThat(streamProcessingConfig.getRetryDelayMultiplier(), is(retryDelayMultiplier));
    }

    @Test
    public void shouldGetTheIdleThreshold() {

        final long milliseconds = 2000L;

        setField(streamProcessingConfig, "idleThresholdMilliseconds", "" + milliseconds);

        assertThat(streamProcessingConfig.getIdleThresholdMilliseconds(), is(milliseconds));
    }
}
