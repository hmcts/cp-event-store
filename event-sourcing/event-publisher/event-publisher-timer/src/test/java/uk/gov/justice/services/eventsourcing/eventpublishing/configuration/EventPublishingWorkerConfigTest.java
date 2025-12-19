package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventPublishingWorkerConfigTest {

    @InjectMocks
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @BeforeEach
    public void postConstruct() {
        setField(eventPublishingWorkerConfig, "eventPublisherNotified", "false");
        setField(eventPublishingWorkerConfig, "backoffMinMilliseconds", "1");
        setField(eventPublishingWorkerConfig, "backoffMaxMilliseconds", "1");
        setField(eventPublishingWorkerConfig, "backoffMultiplier", "1");
    }

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {

        final long milliseconds = 982374L;

        setField(eventPublishingWorkerConfig, "timerStartWaitMilliseconds", "" + milliseconds);

        assertThat(eventPublishingWorkerConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {

        final long milliseconds = 2998734L;

        setField(eventPublishingWorkerConfig, "timerIntervalMilliseconds", "" + milliseconds);

        assertThat(eventPublishingWorkerConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimeBetweenRuns() throws Exception {

        final long milliseconds = 28734L;

        setField(eventPublishingWorkerConfig, "timeBetweenRunsMilliseconds", "" + milliseconds);

        assertThat(eventPublishingWorkerConfig.getTimeBetweenRunsMilliseconds(), is(milliseconds));
    }


    @Test
    public void shouldGetEventLinkerNotified() throws IllegalAccessException {
        setField(eventPublishingWorkerConfig, "eventPublisherNotified", "true");
        eventPublishingWorkerConfig.postConstruct();
        assertThat(eventPublishingWorkerConfig.shouldWorkerNotified(), is(true));

        setField(eventPublishingWorkerConfig, "eventPublisherNotified", "false");
        eventPublishingWorkerConfig.postConstruct();

        assertThat(eventPublishingWorkerConfig.shouldWorkerNotified(), is(false));
    }

    @Test
    public void shouldGetBackoffMinMilliseconds() throws IllegalAccessException {
        long backoffMinMilliseconds = 100L;
        setField(eventPublishingWorkerConfig, "backoffMinMilliseconds", String.valueOf(backoffMinMilliseconds));
        eventPublishingWorkerConfig.postConstruct();
        assertThat(eventPublishingWorkerConfig.getBackoffMinMilliseconds(), is(backoffMinMilliseconds));
    }

    @Test
    public void shouldGetBackoffMaxMilliseconds() throws IllegalAccessException {
        long backoffMaxMilliseconds = 500L;
        setField(eventPublishingWorkerConfig, "backoffMaxMilliseconds", String.valueOf(backoffMaxMilliseconds));
        eventPublishingWorkerConfig.postConstruct();
        assertThat(eventPublishingWorkerConfig.getBackoffMaxMilliseconds(), is(backoffMaxMilliseconds));
    }

    @Test
    public void shouldGetBackoffMultiplier() throws IllegalAccessException {
        double backoffMultiplier = 2.0;
        setField(eventPublishingWorkerConfig, "backoffMultiplier", String.valueOf(backoffMultiplier));
        eventPublishingWorkerConfig.postConstruct();
        assertThat(eventPublishingWorkerConfig.getBackoffMultiplier(), is(backoffMultiplier));
    }

}
