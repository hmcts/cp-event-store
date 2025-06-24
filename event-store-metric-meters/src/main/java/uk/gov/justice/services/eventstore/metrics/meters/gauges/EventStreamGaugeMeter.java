package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.SourceComponentPair;

import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

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
    public Integer get() {
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

    @VisibleForTesting
    public SourceComponentPair sourceComponentPair() {
        return sourceComponentPair;
    }
}
