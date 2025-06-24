package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.FRESH_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.STALE_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.TOTAL_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UNBLOCKED_EVENT_STREAMS_GAUGE_NAME;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.metrics.micrometer.meters.SourceComponentPair;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;

public class GaugeMeterFactory {

    private static final List<GaugeMeterConfig> GAUGE_METER_CONFIGS = Arrays.asList(
            new GaugeMeterConfig(StreamMetrics::blockedStreamCount, BLOCKED_EVENT_STREAMS_GAUGE_NAME, "The current number of streams that are blocked due to errors"),
            new GaugeMeterConfig(StreamMetrics::streamCount, TOTAL_EVENT_STREAMS_GAUGE_NAME, "The total number of event streams"),
            new GaugeMeterConfig(StreamMetrics::outOfDateStreamCount, STALE_EVENT_STREAMS_GAUGE_NAME, "The current number of streams that are out of date"),
            new GaugeMeterConfig(StreamMetrics::unblockedStreamCount, UNBLOCKED_EVENT_STREAMS_GAUGE_NAME, "The current number of streams not in error"),
            new GaugeMeterConfig(StreamMetrics::upToDateStreamCount, FRESH_EVENT_STREAMS_GAUGE_NAME, "The current number of streams that are up to date")
    );

    private record GaugeMeterConfig(Function<StreamMetrics, Integer> metricExtractor,
                                    String metricName,
                                    String metricDescription) {
    }

    @Inject
    private StreamMetricsProvider streamMetricsProvider;


    private Stream<EventStreamGaugeMeter> createAllGaugeMetersForSourceAndComponent(
            SourceComponentPair sourceAndComponent) {

        return GAUGE_METER_CONFIGS.stream()
                .map(config -> new EventStreamGaugeMeter(
                        sourceAndComponent,
                        streamMetricsProvider,
                        config.metricExtractor(),
                        config.metricName(),
                        config.metricDescription()
                ));
    }

    public List<EventStreamGaugeMeter> createAllGaugeMetersForSourceAndComponents(List<SourceComponentPair> sourceAndComponents) {
        return sourceAndComponents.stream()
                .flatMap(this::createAllGaugeMetersForSourceAndComponent)
                .toList();
    }
}
