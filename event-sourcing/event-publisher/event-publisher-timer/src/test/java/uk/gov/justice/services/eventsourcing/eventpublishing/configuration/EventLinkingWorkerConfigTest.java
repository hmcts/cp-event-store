package uk.gov.justice.services.eventsourcing.eventpublishing.configuration;

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

    @Test
    public void shouldGetTheStartWaitTime() throws Exception {
        setField(eventLinkingWorkerConfig, "timerStartWaitMilliseconds", "72374");
        assertThat(eventLinkingWorkerConfig.getTimerStartWaitMilliseconds(), is(72374L));
    }

    @Test
    public void shouldGetTheTimerInterval() throws Exception {
        setField(eventLinkingWorkerConfig, "timerIntervalMilliseconds", "2998734");
        assertThat(eventLinkingWorkerConfig.getTimerIntervalMilliseconds(), is(2998734L));
    }

    @Test
    public void shouldGetTheTimeBetweenRuns() throws Exception {
        setField(eventLinkingWorkerConfig, "timeBetweenRunsMilliseconds", "9982134");
        assertThat(eventLinkingWorkerConfig.getTimeBetweenRunsMilliseconds(), is(9982134L));
    }

    @Test
    public void shouldGetPublishEventToPublishedEventTable() throws Exception {
        setField(eventLinkingWorkerConfig, "insertEventIntoPublishedEventTable", "false");
        assertThat(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(false));

        setField(eventLinkingWorkerConfig, "insertEventIntoPublishedEventTable", "true");
        assertThat(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable(), is(true));
    }

    @Test
    public void shouldGetBatchSize() throws Exception {
        setField(eventLinkingWorkerConfig, "batchSize", "20");
        assertThat(eventLinkingWorkerConfig.getBatchSize(), is(20));
    }

    @Test
    public void shouldGetTransactionTimeoutSeconds() throws Exception {
        setField(eventLinkingWorkerConfig, "transactionTimeoutSeconds", "120");
        assertThat(eventLinkingWorkerConfig.getTransactionTimeoutSeconds(), is(120));
    }

    @Test
    public void shouldGetTransactionStatementTimeoutSeconds() throws Exception {
        setField(eventLinkingWorkerConfig, "transactionStatementTimeoutSeconds", "240");
        assertThat(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds(), is(240));
    }

    @Test
    public void shouldGetEventLinkerNotified() throws Exception {
        setField(eventLinkingWorkerConfig, "eventLinkerNotified", "true");
        assertThat(eventLinkingWorkerConfig.shouldWorkerNotified(), is(true));

        setField(eventLinkingWorkerConfig, "eventLinkerNotified", "false");
        assertThat(eventLinkingWorkerConfig.shouldWorkerNotified(), is(false));
    }
}
