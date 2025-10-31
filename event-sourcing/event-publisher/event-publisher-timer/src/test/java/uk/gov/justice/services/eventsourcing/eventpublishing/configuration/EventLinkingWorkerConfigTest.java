package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerConfigTest {

    @InjectMocks
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {

        final long milliseconds = 72374L;

        setField(eventLinkingWorkerConfig, "timerStartWaitMilliseconds", "" + milliseconds);

        assertThat(eventLinkingWorkerConfig.getTimerStartWaitMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {

        final long milliseconds = 2998734L;

        setField(eventLinkingWorkerConfig, "timerIntervalMilliseconds", "" + milliseconds);

        assertThat(eventLinkingWorkerConfig.getTimerIntervalMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetTheTimeBetweenRuns() throws Exception {

        final long milliseconds = 9982134L;

        setField(eventLinkingWorkerConfig, "timeBetweenRunsMilliseconds", "" + milliseconds);

        assertThat(eventLinkingWorkerConfig.getTimeBetweenRunsMilliseconds(), is(milliseconds));
    }

    @Test
    public void shouldGetPublishEventToPublishedEventTable() throws Exception {

        assertThat(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(false));

        setField(eventLinkingWorkerConfig, "insertEventIntoPublishedEventTable", "true");

        assertThat(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(true));
    }
}
