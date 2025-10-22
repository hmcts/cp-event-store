package uk.gov.justice.services.subscription;

import java.util.stream.Stream;

import javax.inject.Inject;

public class ProcessedEventStreamer {

    @Inject
    private ProcessedEventStreamerConfiguration processedEventStreamerConfiguration;

    @Inject
    private ProcessedEventStreamSpliteratorFactory processedEventStreamSpliteratorFactory;

    @Inject
    private SpliteratorStreamFactory spliteratorStreamFactory;

    public Stream<ProcessedEvent> getProcessedEventStream(final String source, final String component) {

        final Long runFromEventNumber = 1L;
        final Long processedEventFetchBatchSize = processedEventStreamerConfiguration.getProcessedEventFetchBatchSize();
        final ProcessedEventStreamSpliterator processedEventStreamSpliterator = processedEventStreamSpliteratorFactory
                .getProcessedEventStreamSpliterator(
                        source,
                        component,
                        processedEventFetchBatchSize,
                        runFromEventNumber);

        return spliteratorStreamFactory.createStreamFrom(processedEventStreamSpliterator);
    }
}
