package uk.gov.justice.services.eventstore.metrics.meters.gauges;

import static java.lang.String.format;

import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetrics;
import uk.gov.justice.services.event.buffer.core.repository.metrics.StreamMetricsRepository;

import java.util.Optional;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamMetricsProvider {

    @Inject
    private StreamMetricsRepository streamMetricsRepository;

    @Inject
    private Logger logger;

    public Optional<StreamMetrics> getMetrics(final String source, final String component) {
        final Optional<StreamMetrics> streamMetrics = streamMetricsRepository.getStreamMetrics(source, component);

        if (logger.isDebugEnabled()) {
            logger.debug(format("Retrieved stream metrics for %s %s: %s", source, component, streamMetrics));
        }

        return streamMetrics;
    }
}
