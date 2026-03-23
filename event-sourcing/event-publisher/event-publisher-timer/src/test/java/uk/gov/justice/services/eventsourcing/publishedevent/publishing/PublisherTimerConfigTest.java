package uk.gov.justice.services.eventsourcing.publishedevent.publishing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PublisherTimerConfigTest {

    @InjectMocks
    private PublisherTimerConfig publisherTimerConfig;

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {

        final long milliseconds = 982374L;

        publisherTimerConfig.timerStartWaitMilliseconds = "" + milliseconds;

        assertThat(publisherTimerConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {

        final long milliseconds = 2998734L;

        publisherTimerConfig.timerIntervalMilliseconds = "" + milliseconds;

        assertThat(publisherTimerConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerMaxRuntime() throws Exception {

        final long milliseconds = 600L;

        publisherTimerConfig.timerMaxRuntimeMilliseconds = "" + milliseconds;

        assertThat(publisherTimerConfig.getTimerMaxRuntimeMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldReturnTrueIfDisabled() throws Exception {

        setField(publisherTimerConfig, "disablePublish", "TRUE");
        assertThat(publisherTimerConfig.isDisabled(), is(true));

        setField(publisherTimerConfig, "disablePublish", "true");
        assertThat(publisherTimerConfig.isDisabled(), is(true));

        setField(publisherTimerConfig, "disablePublish", "True");
        assertThat(publisherTimerConfig.isDisabled(), is(true));

        setField(publisherTimerConfig, "disablePublish", "FALSE");
        assertThat(publisherTimerConfig.isDisabled(), is(false));

        setField(publisherTimerConfig, "disablePublish", "false");
        assertThat(publisherTimerConfig.isDisabled(), is(false));

        setField(publisherTimerConfig, "disablePublish", "False");
        assertThat(publisherTimerConfig.isDisabled(), is(false));

        setField(publisherTimerConfig, "disablePublish", "something very silly");
        assertThat(publisherTimerConfig.isDisabled(), is(false));

        setField(publisherTimerConfig, "disablePublish", null);
        assertThat(publisherTimerConfig.isDisabled(), is(false));

        publisherTimerConfig.setDisabled(true);
        assertThat(publisherTimerConfig.isDisabled(), is(true));
    }

    @Test
    public void shouldReturnTrueIfWorkerNotified() throws Exception {

        setField(publisherTimerConfig, "eventPublisherNotified", "true");
        assertThat(publisherTimerConfig.shouldWorkerNotified(), is(true));

        setField(publisherTimerConfig, "eventPublisherNotified", "false");
        assertThat(publisherTimerConfig.shouldWorkerNotified(), is(false));
    }
}
