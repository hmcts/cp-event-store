package uk.gov.justice.services.subscription;

import static java.lang.Long.MAX_VALUE;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
public class ProcessedEventTrackingRepositoryIT {

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
    public void shouldSaveAndGetAllProcessedEvents() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final ProcessedEvent processedEvent_1 = new ProcessedEvent(randomUUID(), 0, 1, source, componentName);
        final ProcessedEvent processedEvent_2 = new ProcessedEvent(randomUUID(), 1, 2, source, componentName);
        final ProcessedEvent processedEvent_3 = new ProcessedEvent(randomUUID(), 2, 3, source, componentName);
        final ProcessedEvent processedEvent_4 = new ProcessedEvent(randomUUID(), 3, 4, source, componentName);

        processedEventTrackingRepository.save(processedEvent_1);
        processedEventTrackingRepository.save(processedEvent_2);
        processedEventTrackingRepository.save(processedEvent_3);
        processedEventTrackingRepository.save(processedEvent_4);

        final Stream<ProcessedEvent> allProcessedEvents = processedEventTrackingRepository.getAllProcessedEventsDescendingOrder(source, componentName);

        final List<ProcessedEvent> processedEvents = allProcessedEvents.collect(toList());

        assertThat(processedEvents.size(), is(4));

        assertThat(processedEvents.get(0), is(processedEvent_4));
        assertThat(processedEvents.get(1), is(processedEvent_3));
        assertThat(processedEvents.get(2), is(processedEvent_2));
        assertThat(processedEvents.get(3), is(processedEvent_1));
    }

    @Test
    public void shouldReturnProcessedEventsInDescendingOrderIfInsertedOutOfOrder() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final ProcessedEvent processedEvent_1 = new ProcessedEvent(randomUUID(), 0, 1, source, componentName);
        final ProcessedEvent processedEvent_2 = new ProcessedEvent(randomUUID(), 1, 2, source, componentName);
        final ProcessedEvent processedEvent_3 = new ProcessedEvent(randomUUID(), 2, 3, source, componentName);
        final ProcessedEvent processedEvent_4 = new ProcessedEvent(randomUUID(), 3, 4, source, componentName);

        processedEventTrackingRepository.save(processedEvent_2);
        processedEventTrackingRepository.save(processedEvent_4);
        processedEventTrackingRepository.save(processedEvent_1);
        processedEventTrackingRepository.save(processedEvent_3);

        final Stream<ProcessedEvent> allProcessedEvents = processedEventTrackingRepository.getAllProcessedEventsDescendingOrder(source, componentName);

        final List<ProcessedEvent> processedEvents = allProcessedEvents.collect(toList());

        assertThat(processedEvents.size(), is(4));

        assertThat(processedEvents.get(0), is(processedEvent_4));
        assertThat(processedEvents.get(1), is(processedEvent_3));
        assertThat(processedEvents.get(2), is(processedEvent_2));
        assertThat(processedEvents.get(3), is(processedEvent_1));
    }

    @Test
    public void shouldFetchProcessedEventsFromTheViewstoreInBatches() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long batchSize = 5L;
        final long numberOfEventsToCreate = 17L;
        final Long runFromEventNumber = 1L;

        for (int i = 0; i < numberOfEventsToCreate; i++) {
            final ProcessedEvent processedEvent = new ProcessedEvent(randomUUID(), i, i + 1, source, componentName);
            processedEventTrackingRepository.save(processedEvent);
        }

        long toEventNumber = MAX_VALUE;

        final List<ProcessedEvent> allProcessedEvents = new ArrayList<>();

        while (toEventNumber > 1) {
            final List<ProcessedEvent> processedEvents = processedEventTrackingRepository.getProcessedEventsInBatchesInDescendingOrder(
                    runFromEventNumber,
                    toEventNumber,
                    batchSize,
                    source,
                    componentName);


            if (toEventNumber > batchSize) {
                assertThat((long) processedEvents.size(), is(batchSize));
            } else {
                assertThat((long) processedEvents.size(), is(toEventNumber - 1));
            }

            final ProcessedEvent finalEvent = processedEvents.get(processedEvents.size() - 1);
            toEventNumber = finalEvent.getEventNumber();

            allProcessedEvents.addAll(processedEvents);
        }

        assertThat((long) allProcessedEvents.size(), is(numberOfEventsToCreate));

        long currentEventNumber = numberOfEventsToCreate;

        for (final ProcessedEvent processedEvent : allProcessedEvents) {
            assertThat(processedEvent.getEventNumber(), is(currentEventNumber));
            assertThat(processedEvent.getPreviousEventNumber(), is(currentEventNumber - 1));
            currentEventNumber--;
        }
    }

    @Test
    public void shouldGetProcessedEventsFromTheSpecifiedEventNumber() throws Exception {
        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final Long batchSize = 100L;
        final long numberOfEventsToCreate = 17L;
        final Long runFromEventNumber = 7L;
        final long toEventNumber = MAX_VALUE;

        for (int i = 0; i < numberOfEventsToCreate; i++) {
            final ProcessedEvent processedEvent = new ProcessedEvent(randomUUID(), i, i + 1, source, componentName);
            processedEventTrackingRepository.save(processedEvent);
        }

        final List<ProcessedEvent> processedEvents = processedEventTrackingRepository.getProcessedEventsInBatchesInDescendingOrder(
                runFromEventNumber,
                toEventNumber,
                batchSize,
                source,
                componentName);

        assertThat(processedEvents.size(), is(11));

        assertThat(processedEvents.get(0).getEventNumber(), is(17L));
        assertThat(processedEvents.get(0).getPreviousEventNumber(), is(16L));
        assertThat(processedEvents.get(1).getEventNumber(), is(16L));
        assertThat(processedEvents.get(1).getPreviousEventNumber(), is(15L));
        assertThat(processedEvents.get(2).getEventNumber(), is(15L));
        assertThat(processedEvents.get(2).getPreviousEventNumber(), is(14L));
        assertThat(processedEvents.get(3).getEventNumber(), is(14L));
        assertThat(processedEvents.get(3).getPreviousEventNumber(), is(13L));
        assertThat(processedEvents.get(4).getEventNumber(), is(13L));
        assertThat(processedEvents.get(4).getPreviousEventNumber(), is(12L));
        assertThat(processedEvents.get(5).getEventNumber(), is(12L));
        assertThat(processedEvents.get(5).getPreviousEventNumber(), is(11L));
        assertThat(processedEvents.get(6).getEventNumber(), is(11L));
        assertThat(processedEvents.get(6).getPreviousEventNumber(), is(10L));
        assertThat(processedEvents.get(7).getEventNumber(), is(10L));
        assertThat(processedEvents.get(7).getPreviousEventNumber(), is(9L));
        assertThat(processedEvents.get(8).getEventNumber(), is(9L));
        assertThat(processedEvents.get(8).getPreviousEventNumber(), is(8L));
        assertThat(processedEvents.get(9).getEventNumber(), is(8L));
        assertThat(processedEvents.get(9).getPreviousEventNumber(), is(7L));

    }

    @Test
    public void shouldReturnOnlyProcessedEventsWIthTheCorrectSourceInDescendingOrder() throws Exception {

        final String source = "example-context";
        final String otherSource = "another-context";
        final String componentName = "EVENT_LISTENER";

        final ProcessedEvent processedEvent_1 = new ProcessedEvent(randomUUID(), 0, 1, source, componentName);
        final ProcessedEvent processedEvent_2 = new ProcessedEvent(randomUUID(), 1, 2, source, componentName);
        final ProcessedEvent processedEvent_3 = new ProcessedEvent(randomUUID(), 2, 3, source, componentName);
        final ProcessedEvent processedEvent_4 = new ProcessedEvent(randomUUID(), 3, 4, source, componentName);

        final ProcessedEvent processedEvent_5 = new ProcessedEvent(randomUUID(), 0, 1, otherSource, componentName);
        final ProcessedEvent processedEvent_6 = new ProcessedEvent(randomUUID(), 1, 2, otherSource, componentName);
        final ProcessedEvent processedEvent_7 = new ProcessedEvent(randomUUID(), 2, 3, otherSource, componentName);
        final ProcessedEvent processedEvent_8 = new ProcessedEvent(randomUUID(), 3, 4, otherSource, componentName);
        final ProcessedEvent processedEvent_9 = new ProcessedEvent(randomUUID(), 5, 6, otherSource, componentName);

        processedEventTrackingRepository.save(processedEvent_2);
        processedEventTrackingRepository.save(processedEvent_4);
        processedEventTrackingRepository.save(processedEvent_1);
        processedEventTrackingRepository.save(processedEvent_6);
        processedEventTrackingRepository.save(processedEvent_7);
        processedEventTrackingRepository.save(processedEvent_5);
        processedEventTrackingRepository.save(processedEvent_8);
        processedEventTrackingRepository.save(processedEvent_3);
        processedEventTrackingRepository.save(processedEvent_9);

        final Stream<ProcessedEvent> allProcessedEvents = processedEventTrackingRepository.getAllProcessedEventsDescendingOrder(source, componentName);

        final List<ProcessedEvent> processedEvents = allProcessedEvents.collect(toList());

        assertThat(processedEvents.size(), is(4));

        assertThat(processedEvents.get(0), is(processedEvent_4));
        assertThat(processedEvents.get(1), is(processedEvent_3));
        assertThat(processedEvents.get(2), is(processedEvent_2));
        assertThat(processedEvents.get(3), is(processedEvent_1));
    }

    @Test
    public void shouldGetTheLatestProcessedEvent() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";

        final ProcessedEvent processedEvent_1 = new ProcessedEvent(randomUUID(), 0, 1, source, componentName);
        final ProcessedEvent processedEvent_2 = new ProcessedEvent(randomUUID(), 1, 2, source, componentName);
        final ProcessedEvent processedEvent_3 = new ProcessedEvent(randomUUID(), 2, 3, source, componentName);
        final ProcessedEvent processedEvent_4 = new ProcessedEvent(randomUUID(), 3, 4, source, componentName);
        final ProcessedEvent processedEvent_5 = new ProcessedEvent(randomUUID(), 99, 100, "a-different-context", componentName);

        processedEventTrackingRepository.save(processedEvent_2);
        processedEventTrackingRepository.save(processedEvent_5);
        processedEventTrackingRepository.save(processedEvent_4);
        processedEventTrackingRepository.save(processedEvent_1);
        processedEventTrackingRepository.save(processedEvent_3);

        final Optional<ProcessedEvent> latestProcessedEvent = processedEventTrackingRepository.getLatestProcessedEvent(source, componentName);

        if (latestProcessedEvent.isPresent()) {

            final ProcessedEvent processedEvent = latestProcessedEvent.get();

            assertThat(processedEvent.getEventNumber(), is(4L));
            assertThat(processedEvent.getPreviousEventNumber(), is(3L));
            assertThat(processedEvent.getSource(), is(source));

        } else {
            fail();
        }
    }

    @Test
    public void shouldThrowProcessedEventTrackingExceptionOnSaveIfEventNumberSourceOrComponentAreNotUnique() throws Exception {

        final String source = "example-context";
        final String componentName = "EVENT_LISTENER";
        final UUID failingEventId = fromString("ed1ddaf4-f9cc-495c-b06b-bc8f638f080d");

        final ProcessedEvent processedEvent_1 = new ProcessedEvent(randomUUID(), 0, 1, source, componentName);
        final ProcessedEvent processedEvent_2 = new ProcessedEvent(failingEventId, 0, 1, source, componentName);

        processedEventTrackingRepository.save(processedEvent_1);

        final ProcessedEventTrackingException processedEventTrackingException = assertThrows(
                ProcessedEventTrackingException.class,
                () -> processedEventTrackingRepository.save(processedEvent_2));

        assertThat(processedEventTrackingException.getMessage(), startsWith("Failed to insert event with id 'ed1ddaf4-f9cc-495c-b06b-bc8f638f080d' into processed_event table. 'event_number', 'source' and 'component' must be unique:"));

    }
}
