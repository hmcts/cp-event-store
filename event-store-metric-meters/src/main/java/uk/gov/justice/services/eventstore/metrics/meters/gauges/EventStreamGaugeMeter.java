package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.COMPONENT_TAG_NAME;
import static uk.gov.justice.services.eventstore.metrics.tags.TagNames.SOURCE_TAG_NAME;

import io.micrometer.core.instrument.Tag;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.eventstore.metrics.tags.TagProvider.SourceComponentPair;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import java.util.List;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

public class EventStreamGaugeMeter implements GaugeMetricsMeter {

    private final SourceComponentPair sourceComponentPair;
    private final StreamMetricsProvider streamMetricsProvider;
    private final Function<StreamMetrics, Integer> metricExtractor;
    private final String metricName;
    private final String metricDescription;

    public EventStreamGaugeMeter(
            final SourceComponentPair sourceComponentPair,
            final StreamMetricsProvider streamMetricsProvider,
            final Function<StreamMetrics, Integer> metricExtractor,
            final String metricName,
            final String metricDescription) {
        this.sourceComponentPair = sourceComponentPair;
        this.streamMetricsProvider = streamMetricsProvider;
        this.metricExtractor = metricExtractor;
        this.metricName = metricName;
        this.metricDescription = metricDescription;
    }

    @Override
    public int measure() {
        return streamMetricsProvider
                .getMetrics(sourceComponentPair.source(), sourceComponentPair.component())
                .map(metricExtractor)
                .orElse(0);
    }

    @Override
    public String metricName() {
        return metricName;
    }

    @Override
    public String metricDescription() {
        return metricDescription;
    }

    @Override
    public List<Tag> metricTags() {
        return List.of(Tag.of(SOURCE_TAG_NAME.getTagName(), sourceComponentPair.source()), 
                       Tag.of(COMPONENT_TAG_NAME.getTagName(), sourceComponentPair.component()));
    }

    @VisibleForTesting
    public SourceComponentPair getSourceComponentPair() {
        return sourceComponentPair;
    }
}
