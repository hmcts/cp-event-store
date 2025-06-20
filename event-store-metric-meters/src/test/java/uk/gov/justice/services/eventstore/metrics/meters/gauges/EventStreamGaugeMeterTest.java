package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static java.util.Optional.of;
import static java.util.Optional.empty;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.COMPONENT_TAG_NAME;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.SOURCE_TAG_NAME;

import uk.gov.justice.services.eventstore.metrics.tags.TagProvider.SourceComponentPair;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;

import java.util.List;
import java.util.function.Function;

import io.micrometer.core.instrument.Tag;

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

        assertThat(gaugeMeter.measure(), is(expectedValue));
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

        assertThat(gaugeMeter.measure(), is(0));
    }

    @Test
    public void shouldReturnSourceAndComponentTags() {
        final Function<StreamMetrics, Integer> metricExtractor = metrics -> 42;
        final SourceComponentPair sourceComponentPair = new SourceComponentPair(SOURCE, COMPONENT);

        final EventStreamGaugeMeter gaugeMeter = new EventStreamGaugeMeter(
                sourceComponentPair,
                streamMetricsProvider,
                metricExtractor,
                METRIC_NAME,
                METRIC_DESCRIPTION
        );

        final List<Tag> tags = gaugeMeter.metricTags();

        assertThat(tags.size(), is(2));
        assertThat(tags.get(0).getKey(), is(SOURCE_TAG_NAME.getTagName()));
        assertThat(tags.get(0).getValue(), is(SOURCE));
        assertThat(tags.get(1).getKey(), is(COMPONENT_TAG_NAME.getTagName()));
        assertThat(tags.get(1).getValue(), is(COMPONENT));
    }
}
