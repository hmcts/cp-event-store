package uk.gov.justice.services.subscription;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.core.reflection.ReflectionUtil.getValueOfField;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class ProcessedEventStreamSpliteratorFactoryTest {

    @Mock
    private ProcessedEventTrackingRepository processedEventTrackingRepository;
    
    @InjectMocks
    private ProcessedEventStreamSpliteratorFactory processedEventStreamSpliteratorFactory;

    @Test
    public void shouldCreateProcessedEventStreamSpliterator() throws Exception {

        final String source = "source";
        final String component = "component";
        final long batchSize = 23L;
        final long runFromEventNumber = 4L;

        final ProcessedEventStreamSpliterator processedEventStreamSpliterator = processedEventStreamSpliteratorFactory.getProcessedEventStreamSpliterator(
                source,
                component,
                batchSize,
                runFromEventNumber);

        assertThat(processedEventStreamSpliterator, is(notNullValue()));
        assertThat(getValueOfField(processedEventStreamSpliterator, "source", String.class), is(source));
        assertThat(getValueOfField(processedEventStreamSpliterator, "component", String.class), is(component));
        assertThat(getValueOfField(processedEventStreamSpliterator, "batchSize", Long.class), is(batchSize));
        assertThat(getValueOfField(processedEventStreamSpliterator, "processedEventTrackingRepository", ProcessedEventTrackingRepository.class), is(processedEventTrackingRepository));
        assertThat(getValueOfField(processedEventStreamSpliterator, "runFromEventNumber", Long.class), is(runFromEventNumber));
    }
}