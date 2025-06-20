package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.BLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.FRESH_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.STALE_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.TOTAL_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.metrics.micrometer.meters.MetricsMeterNames.UNBLOCKED_EVENT_STREAMS_GAUGE_NAME;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import uk.gov.justice.services.eventstore.metrics.tags.TagProvider.SourceComponentPair;
import uk.gov.justice.services.metrics.micrometer.meters.GaugeMetricsMeter;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class GaugeMeterFactoryTest {

    @Mock
    private StreamMetricsProvider streamMetricsProvider;

    @InjectMocks
    private GaugeMeterFactory gaugeMeterFactory;


    @Test
    public void shouldCreateAllGaugeMetersForSingleSourceAndComponent() throws Exception {
        final String source = "some-source";
        final String component = "some-component";

        final List<SourceComponentPair> sourceAndComponents = List.of(new SourceComponentPair(source, component));

        // run
        final List<EventStreamGaugeMeter> gaugeMeters = gaugeMeterFactory.createAllGaugeMetersForSourceAndComponents(sourceAndComponents);

        assertThat(gaugeMeters, hasSize(5));

        // Verify each gauge meter is an instance of EventStreamGaugeMeter with correct properties
        for (EventStreamGaugeMeter gaugeMeter : gaugeMeters) {
            SourceComponentPair sourceComponentPair = gaugeMeter.getSourceComponentPair();
            assertThat(sourceComponentPair.source(), is(source));
            assertThat(sourceComponentPair.component(), is(component));
            assertThat(getValueOfField(gaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
        }

        // Verify specific properties of each gauge meter
        assertThat(gaugeMeters.get(0).metricName(), is(BLOCKED_EVENT_STREAMS_GAUGE_NAME));
        assertThat(gaugeMeters.get(1).metricName(), is(TOTAL_EVENT_STREAMS_GAUGE_NAME));
        assertThat(gaugeMeters.get(2).metricName(), is(STALE_EVENT_STREAMS_GAUGE_NAME));
        assertThat(gaugeMeters.get(3).metricName(), is(UNBLOCKED_EVENT_STREAMS_GAUGE_NAME));
        assertThat(gaugeMeters.get(4).metricName(), is(FRESH_EVENT_STREAMS_GAUGE_NAME));
    }

    @Test
    public void shouldCreateAllGaugeMetersForMultipleSourceAndComponents() throws Exception {
        final String source1 = "source-1";
        final String component1 = "component-1";
        final String source2 = "source-2";
        final String component2 = "component-2";

        final List<SourceComponentPair> sourceAndComponents = List.of(
                new SourceComponentPair(source1, component1),
                new SourceComponentPair(source2, component2)
        );

        // run
        final List<EventStreamGaugeMeter> gaugeMeters = gaugeMeterFactory.createAllGaugeMetersForSourceAndComponents(sourceAndComponents);

        // verify
        assertThat(gaugeMeters, hasSize(10));

        // Verify first 5 meters are for source1/component1
        for (int i = 0; i < 5; i++) {
            EventStreamGaugeMeter eventStreamGaugeMeter = gaugeMeters.get(i);
            SourceComponentPair sourceComponentPair = eventStreamGaugeMeter.getSourceComponentPair();
            assertThat(sourceComponentPair.source(), is(source1));
            assertThat(sourceComponentPair.component(), is(component1));
            assertThat(getValueOfField(eventStreamGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
        }

        // Verify next 5 meters are for source2/component2
        for (int i = 5; i < 10; i++) {
            EventStreamGaugeMeter eventStreamGaugeMeter = gaugeMeters.get(i);
            SourceComponentPair sourceComponentPair = eventStreamGaugeMeter.getSourceComponentPair();
            assertThat(sourceComponentPair.source(), is(source2));
            assertThat(sourceComponentPair.component(), is(component2));
            assertThat(getValueOfField(eventStreamGaugeMeter, "streamMetricsProvider", StreamMetricsProvider.class), is(streamMetricsProvider));
        }
    }
}
