package uk.gov.justice.eventsourcing.discovery.timers;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryTimerConfigTest {

    @InjectMocks
    private EventDiscoveryTimerConfig eventDiscoveryTimerConfig;


    @Test
    public void shouldGetTimerStartWaitMilliseconds() throws Exception {

        final long timerStartWaitMilliseconds = 23L;

        setField(eventDiscoveryTimerConfig, "timerStartWaitMilliseconds", "" + timerStartWaitMilliseconds);

        assertThat(eventDiscoveryTimerConfig.getTimerStartWaitMilliseconds(), is(timerStartWaitMilliseconds));
    }

    @Test
    public void shouldGetTimerIntervalMillisecond() throws Exception {

        final long timerIntervalMilliseconds = 23L;

        setField(eventDiscoveryTimerConfig, "timerIntervalMilliseconds", "" + timerIntervalMilliseconds);

        assertThat(eventDiscoveryTimerConfig.getTimerIntervalMilliseconds(), is(timerIntervalMilliseconds));
    }

    @Test
    public void shouldGetTimeBetweenRunsMilliseconds() throws Exception {

        final long timeBetweenRunsMilliseconds = 23L;

        setField(eventDiscoveryTimerConfig, "timeBetweenRunsMilliseconds", "" + timeBetweenRunsMilliseconds);

        assertThat(eventDiscoveryTimerConfig.getTimeBetweenRunsMilliseconds(), is(timeBetweenRunsMilliseconds));
    }
}