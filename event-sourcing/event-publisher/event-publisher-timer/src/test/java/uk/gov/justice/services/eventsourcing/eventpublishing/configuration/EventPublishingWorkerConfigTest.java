package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventPublishingWorkerConfigTest {

    @InjectMocks
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {
        setField(eventPublishingWorkerConfig, "timerStartWaitMilliseconds", "982374");
        assertThat(eventPublishingWorkerConfig.getTimerStartWaitMilliseconds(), is(982374L));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {
        setField(eventPublishingWorkerConfig, "timerIntervalMilliseconds", "2998734");
        assertThat(eventPublishingWorkerConfig.getTimerIntervalMilliseconds(), is(2998734L));
    }

    @Test
    public void shouldGetTheTimeBetweenRuns() throws Exception {
        setField(eventPublishingWorkerConfig, "timeBetweenRunsMilliseconds", "28734");
        assertThat(eventPublishingWorkerConfig.getTimeBetweenRunsMilliseconds(), is(28734L));
    }

    @Test
    public void shouldGetEventPublisherNotified() throws Exception {
        setField(eventPublishingWorkerConfig, "eventPublisherNotified", "true");
        assertThat(eventPublishingWorkerConfig.shouldWorkerNotified(), is(true));

        setField(eventPublishingWorkerConfig, "eventPublisherNotified", "false");
        assertThat(eventPublishingWorkerConfig.shouldWorkerNotified(), is(false));
    }
}
