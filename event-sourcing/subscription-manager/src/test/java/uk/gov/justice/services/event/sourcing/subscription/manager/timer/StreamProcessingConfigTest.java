package uk.gov.justice.services.event.sourcing.subscription.manager.timer;

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
    public void shouldGetTheMaxThreads() {

        final int maxThreads = 5;

        setField(streamProcessingConfig, "maxThreads", "" + maxThreads);

        assertThat(streamProcessingConfig.getMaxThreads(), is(maxThreads));
    }
}
