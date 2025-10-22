package uk.gov.justice.services.subscription;

import static java.util.UUID.randomUUID;
import static java.util.stream.StreamSupport.stream;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.util.List;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProcessedEventStreamSpliteratorIT {

    private final DataSource viewStoreDataSource = new FrameworkTestDataSourceFactory().createViewStoreDataSource();

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @SuppressWarnings("unused")
    @Spy
    private JdbcResultSetStreamer jdbcResultSetStreamer = new JdbcResultSetStreamer();

    @SuppressWarnings("unused")
    @Spy
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory = new PreparedStatementWrapperFactory();

    @InjectMocks
    private ProcessedEventTrackingRepository processedEventTrackingRepository;

    @BeforeEach
    public void ensureOurDatasourceProviderReturnsOurTestDataSource() {
        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(viewStoreDataSource);
    }

    @BeforeEach
    public void cleanTable() {
        new DatabaseCleaner().cleanViewStoreTables("framework", "processed_event");
    }

    @Test
    public void shouldUseSpliteratorToCreateSingleStreamOfProcessedEventsFromMultipleDatabaseSelects() throws Exception {
        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long batchSize = 5L;
        final long numberOfEventsToCreate = 17L;
        final Long runFromEventNumber = 1L;

        for (int i = 0; i < numberOfEventsToCreate; i++) {
            final ProcessedEvent processedEvent = new ProcessedEvent(randomUUID(), i, i + 1, source, componentName);
            processedEventTrackingRepository.save(processedEvent);
        }

        try (final Stream<ProcessedEvent> processedEventStream = stream(
                new ProcessedEventStreamSpliterator(source, componentName, batchSize, runFromEventNumber, processedEventTrackingRepository),
                false)) {
            final List<ProcessedEvent> processedEvents = processedEventStream.toList();
            long currentEventNumber = numberOfEventsToCreate;

            assertThat(processedEvents.size(), is(17));
            for (final ProcessedEvent processedEvent : processedEvents) {
                assertThat(processedEvent.getEventNumber(), is(currentEventNumber));
                assertThat(processedEvent.getPreviousEventNumber(), is(currentEventNumber - 1));
                currentEventNumber--;
            }
        }
    }
}
