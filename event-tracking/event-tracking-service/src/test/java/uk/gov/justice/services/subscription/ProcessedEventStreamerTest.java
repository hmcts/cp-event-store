package uk.gov.justice.services.subscription;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProcessedEventStreamerTest {

    @Mock
    private ProcessedEventStreamerConfiguration processedEventStreamerConfiguration;

    @Mock
    private ProcessedEventStreamSpliteratorFactory processedEventStreamSpliteratorFactory;

    @Mock
    private SpliteratorStreamFactory spliteratorStreamFactory;

    @InjectMocks
    private ProcessedEventStreamer processedEventStreamer;

    @Test
    public void shouldGetSteamOfProcessedEventsUsingSpliterator() throws Exception {

        final Long batchSize = 10_000L;
        final String source = "THE_SOURCE";
        final String componentName = "EVENT_LISTENER";
        final Long runFromEventNumber = 1L;

        final ProcessedEvent processedEvent = mock(ProcessedEvent.class);
        final ProcessedEventStreamSpliterator processedEventStreamSpliterator = mock(ProcessedEventStreamSpliterator.class);

        when(processedEventStreamerConfiguration.getProcessedEventFetchBatchSize()).thenReturn(batchSize);
        when(processedEventStreamSpliteratorFactory.getProcessedEventStreamSpliterator(source, componentName, batchSize, runFromEventNumber))
                .thenReturn(processedEventStreamSpliterator);
        when(spliteratorStreamFactory.createStreamFrom(processedEventStreamSpliterator)).thenReturn(Stream.of(processedEvent));

        final List<ProcessedEvent> processedEvents = processedEventStreamer.getProcessedEventStream(source, componentName, runFromEventNumber).toList();

        assertThat(processedEvents.size(), is(1));
        assertThat(processedEvents.get(0), is(processedEvent));
    }
}