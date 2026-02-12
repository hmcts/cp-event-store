package uk.gov.justice.services.event.sourcing.subscription.manager;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamRetryConfigurationTest {

    @InjectMocks
    private StreamRetryConfiguration streamRetryConfiguration;

    @Test
    public void shouldParseMaxRetriesStringAndReturnAsInteger() throws Exception {

        final Integer maxRetries = 23;
        setField(streamRetryConfiguration, "maxRetries", maxRetries + "");

        assertThat(streamRetryConfiguration.getMaxRetries(), is(maxRetries));
    }

    @Test
    public void shouldCacheMaxRetriesOnceParsed() throws Exception {

        final Integer maxRetries = 42;

        setField(streamRetryConfiguration, "maxRetries", maxRetries + "");
        assertThat(streamRetryConfiguration.getMaxRetries(), is(maxRetries));

        setField(streamRetryConfiguration, "maxRetries", 66 + "");
        assertThat(streamRetryConfiguration.getMaxRetries(), is(maxRetries));

        setField(streamRetryConfiguration, "maxRetries", 111 + "");
        assertThat(streamRetryConfiguration.getMaxRetries(), is(maxRetries));
    }

    @Test
    public void shouldParseRetryDelayStringAndReturnAsLong() throws Exception {

        final Long retryDelay = 75L;
        setField(streamRetryConfiguration, "retryDelayMilliseconds", retryDelay + "");

        assertThat(streamRetryConfiguration.getRetryDelayMilliseconds(), is(retryDelay));
    }

    @Test
    public void shouldCacheRetryDelayOnceParsed() throws Exception {

        final Long retryDelay = 32L;

        setField(streamRetryConfiguration, "retryDelayMilliseconds", retryDelay + "");
        assertThat(streamRetryConfiguration.getRetryDelayMilliseconds(), is(retryDelay));

        setField(streamRetryConfiguration, "retryDelayMilliseconds", 55 + "");
        assertThat(streamRetryConfiguration.getRetryDelayMilliseconds(), is(retryDelay));

        setField(streamRetryConfiguration, "retryDelayMilliseconds", 87 + "");
        assertThat(streamRetryConfiguration.getRetryDelayMilliseconds(), is(retryDelay));
    }

    @Test
    public void shouldParseRetryDelayMultiplierStringAndReturnAsBigDecimal() throws Exception {

        final BigDecimal retryDelayMultiplier = new BigDecimal("1.23");
        setField(streamRetryConfiguration, "retryDelayMultiplier", retryDelayMultiplier + "");

        assertThat(streamRetryConfiguration.getRetryDelayMultiplier(), is(retryDelayMultiplier));
    }

    @Test
    public void shouldCacheRetryDelayMultiplierOnceParsed() throws Exception {

        final BigDecimal retryDelayMultiplier = new BigDecimal("3.142");

        setField(streamRetryConfiguration, "retryDelayMultiplier", retryDelayMultiplier + "");
        assertThat(streamRetryConfiguration.getRetryDelayMultiplier(), is(retryDelayMultiplier));

        setField(streamRetryConfiguration, "retryDelayMultiplier", "5.322");
        assertThat(streamRetryConfiguration.getRetryDelayMultiplier(), is(retryDelayMultiplier));

        setField(streamRetryConfiguration, "retryDelayMultiplier", "2.887");
        assertThat(streamRetryConfiguration.getRetryDelayMultiplier(), is(retryDelayMultiplier));
    }
}
