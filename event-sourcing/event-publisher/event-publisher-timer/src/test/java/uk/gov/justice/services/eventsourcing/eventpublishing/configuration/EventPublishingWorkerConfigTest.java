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
    public void shouldGetPublishEventToPublishedEventTable() throws Exception {

        assertThat(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(false));

        setField(eventPublishingWorkerConfig, "insertEventIntoPublishedEventTable", "true");

        assertThat(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(true));
    }
}
