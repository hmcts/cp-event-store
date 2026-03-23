package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PrePublisherTimerConfigTest {

    @InjectMocks
    private PrePublisherTimerConfig prePublisherTimerConfig;

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {

        final long milliseconds = 982374L;

        setField(prePublisherTimerConfig, "timerStartWaitMilliseconds", "" + milliseconds);

        assertThat(prePublisherTimerConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {

        final long milliseconds = 2998734L;

        setField(prePublisherTimerConfig, "timerIntervalMilliseconds", "" + milliseconds);

        assertThat(prePublisherTimerConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerMaxRuntime() throws Exception {

        final long milliseconds = 600L;

        setField(prePublisherTimerConfig, "timerMaxRuntimeMilliseconds", "" + milliseconds);

        assertThat(prePublisherTimerConfig.getTimerMaxRuntimeMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldReturnTrueIfDisabled() throws Exception {

        setField(prePublisherTimerConfig, "disablePrePublish", "TRUE");
        assertThat(prePublisherTimerConfig.isDisabled(), is(true));

        setField(prePublisherTimerConfig, "disablePrePublish", "true");
        assertThat(prePublisherTimerConfig.isDisabled(), is(true));

        setField(prePublisherTimerConfig, "disablePrePublish", "True");
        assertThat(prePublisherTimerConfig.isDisabled(), is(true));

        setField(prePublisherTimerConfig, "disablePrePublish", "FALSE");
        assertThat(prePublisherTimerConfig.isDisabled(), is(false));

        setField(prePublisherTimerConfig, "disablePrePublish", "false");
        assertThat(prePublisherTimerConfig.isDisabled(), is(false));

        setField(prePublisherTimerConfig, "disablePrePublish", "False");
        assertThat(prePublisherTimerConfig.isDisabled(), is(false));

        setField(prePublisherTimerConfig, "disablePrePublish", "something very silly");
        assertThat(prePublisherTimerConfig.isDisabled(), is(false));

        setField(prePublisherTimerConfig, "disablePrePublish", null);
        assertThat(prePublisherTimerConfig.isDisabled(), is(false));

        prePublisherTimerConfig.setDisabled(true);

        assertThat(prePublisherTimerConfig.isDisabled(), is(true));
    }

    @Test
    public void shouldReturnTrueIfWorkerNotified() throws Exception {

        setField(prePublisherTimerConfig, "eventLinkerNotified", "true");
        assertThat(prePublisherTimerConfig.shouldWorkerNotified(), is(true));

        setField(prePublisherTimerConfig, "eventLinkerNotified", "false");
        assertThat(prePublisherTimerConfig.shouldWorkerNotified(), is(false));
    }
}
