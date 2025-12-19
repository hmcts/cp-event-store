package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.setField;

@ExtendWith(MockitoExtension.class)
public class EventLinkingWorkerConfigTest {

    @InjectMocks
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @BeforeEach
    public void postConstruct() {
        setField(eventLinkingWorkerConfig, "eventLinkerNotified", "false");
        setField(eventLinkingWorkerConfig, "backoffMinMilliseconds", "1");
        setField(eventLinkingWorkerConfig, "backoffMaxMilliseconds", "1");
        setField(eventLinkingWorkerConfig, "backoffMultiplier", "1");
    }

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

    @Test
    public void shouldGetTransactionTimeoutSeconds() throws Exception {

        final int timeoutSeconds = 120;

        setField(eventLinkingWorkerConfig, "transactionTimeoutSeconds", "" + timeoutSeconds);

        assertThat(eventLinkingWorkerConfig.getTransactionTimeoutSeconds(), is(timeoutSeconds));
    }

    @Test
    public void shouldGetTransactionStatementTimeoutSeconds() throws Exception {

        final int timeoutSeconds = 240;

        setField(eventLinkingWorkerConfig, "transactionStatementTimeoutSeconds", "" + timeoutSeconds);

        assertThat(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds(), is(timeoutSeconds));
    }

    @Test
    public void shouldGetEventLinkerNotified() throws IllegalAccessException {
        setField(eventLinkingWorkerConfig, "eventLinkerNotified", "true");
        eventLinkingWorkerConfig.postConstruct();
        assertThat(eventLinkingWorkerConfig.shouldWorkerNotified(), is(true));

        setField(eventLinkingWorkerConfig, "eventLinkerNotified", "false");
        eventLinkingWorkerConfig.postConstruct();

        assertThat(eventLinkingWorkerConfig.shouldWorkerNotified(), is(false));
    }

    @Test
    public void shouldGetBackoffMinMilliseconds() throws IllegalAccessException {
        long backoffMinMilliseconds = 100L;
        setField(eventLinkingWorkerConfig, "backoffMinMilliseconds", String.valueOf(backoffMinMilliseconds));
        eventLinkingWorkerConfig.postConstruct();
        assertThat(eventLinkingWorkerConfig.getBackoffMinMilliseconds(), is(backoffMinMilliseconds));
    }

    @Test
    public void shouldGetBackoffMaxMilliseconds() throws IllegalAccessException {
        long backoffMaxMilliseconds = 500L;
        setField(eventLinkingWorkerConfig, "backoffMaxMilliseconds", String.valueOf(backoffMaxMilliseconds));
        eventLinkingWorkerConfig.postConstruct();
        assertThat(eventLinkingWorkerConfig.getBackoffMaxMilliseconds(), is(backoffMaxMilliseconds));
    }

    @Test
    public void shouldGetBackoffMultiplier() throws IllegalAccessException {
        double backoffMultiplier = 2.0;
        setField(eventLinkingWorkerConfig, "backoffMultiplier", String.valueOf(backoffMultiplier));
        eventLinkingWorkerConfig.postConstruct();
        assertThat(eventLinkingWorkerConfig.getBackoffMultiplier(), is(backoffMultiplier));
    }
}
