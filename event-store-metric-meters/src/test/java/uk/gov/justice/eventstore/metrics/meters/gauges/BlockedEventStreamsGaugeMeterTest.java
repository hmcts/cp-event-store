package uk.gov.justice.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.COUNT_EVENT_STREAMS_GAUGE_NAME;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class BlockedEventStreamsGaugeMeterTest {

    @Mock
    private EventMetricsRepository eventMetricsRepository;

    @Mock
    private Logger logger;

    @InjectMocks
    private BlockedEventStreamsGaugeMeter blockedEventStreamsGaugeMeter;

    @Test
    public void shouldGetTheCountOfTheTheNumberOfEventStreams() throws Exception {

        final Integer numberOfBlockedStreams = 266;

        when(eventMetricsRepository.countBlockedStreams()).thenReturn(numberOfBlockedStreams);
        when(logger.isDebugEnabled()).thenReturn(false);

        assertThat(blockedEventStreamsGaugeMeter.measure(), is(numberOfBlockedStreams));

        verify(logger, never()).debug(anyString());
    }

    @Test
    public void shouldLogNumberOfStreamsIfDebugIsEnabled() throws Exception {

        final Integer numberOfBlockedStreams = 23;

        when(eventMetricsRepository.countBlockedStreams()).thenReturn(numberOfBlockedStreams);
        when(logger.isDebugEnabled()).thenReturn(true);

        assertThat(blockedEventStreamsGaugeMeter.measure(), is(numberOfBlockedStreams));

        verify(logger).debug("Micrometer counting number of blocked event streams. Number of blocked streams: 23");
    }

    @Test
    public void shouldGetTheCorrectMeterName() throws Exception {

        assertThat(blockedEventStreamsGaugeMeter.metricName(), is(BLOCKED_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldGetTheCorrectMeterDescription() throws Exception {

        assertThat(blockedEventStreamsGaugeMeter.metricDescription(), is("The current number of streams that are blocked due to errors"));
    }
}