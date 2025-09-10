package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.SourceComponentPair;

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventStreamGaugeMeterTest {

    private static final String SOURCE = "some-source";
    private static final String COMPONENT = "some-component";
    private static final String METRIC_NAME = "test.metric.name";
    private static final String METRIC_DESCRIPTION = "Test metric description";

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @Mock
    private StreamMetrics streamMetrics;

    @Test
    public void shouldMeasureUsingProvidedMetricExtractor() {
        final int expectedValue = 42;
        final Function<StreamMetrics, Integer> metricExtractor = metrics -> expectedValue;
        final SourceComponentPair sourceComponentPair = new SourceComponentPair(SOURCE, COMPONENT);

        when(streamMetricsProvider.getMetrics(SOURCE, COMPONENT)).thenReturn(of(streamMetrics));

        final EventStreamGaugeMeter gaugeMeter = new EventStreamGaugeMeter(
                sourceComponentPair,
                streamMetricsProvider,
                metricExtractor,
                METRIC_NAME,
                METRIC_DESCRIPTION
        );

        assertThat(gaugeMeter.get(), is(expectedValue));
        assertThat(gaugeMeter.metricName(), is(METRIC_NAME));
        assertThat(gaugeMeter.metricDescription(), is(METRIC_DESCRIPTION));
    }

    @Test
    public void shouldReturnZeroWhenNoMetricsAvailable() {
        final Function<StreamMetrics, Integer> metricExtractor = metrics -> 42;
        final SourceComponentPair sourceComponentPair = new SourceComponentPair(SOURCE, COMPONENT);

        when(streamMetricsProvider.getMetrics(SOURCE, COMPONENT)).thenReturn(empty());

        final EventStreamGaugeMeter gaugeMeter = new EventStreamGaugeMeter(
                sourceComponentPair,
                streamMetricsProvider,
                metricExtractor,
                METRIC_NAME,
                METRIC_DESCRIPTION
        );

        assertThat(gaugeMeter.get(), is(0));
    }
}
