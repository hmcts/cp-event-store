package uk.gov.justice.services.eventsourcing.repository.jdbc.discovery;

import static java.util.Optional.ofNullable;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventDiscoveryRepositoryIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventDiscoveryRepository eventDiscoveryRepository;

    @BeforeEach
    public void cleanDatabase() {
        new DatabaseCleaner().cleanEventStoreTables("framework");
    }

    @Test
    public void shouldFindMaxPositionInStreamForAllStreamsWithEventNumberGreaterThanEventNumberOfEventId() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId_1 = fromString("100bdbb4-da77-4f9f-b461-3980e3170001");
        final UUID streamId_2 = fromString("200bdbb4-da77-4f9f-b461-3980e3170002");
        final long maxPositionInStream_stream_1 = 11L;
        final long maxPositionInStream_stream_2 = 22L;
        final int batchSize = 100;

        final Event event_1_1 = anEvent(streamId_1, 1L, "event_1_stream_1", 1L);
        final Event event_1_2 = anEvent(streamId_1, maxPositionInStream_stream_1, "event_2_stream_2", 2L);
        final Event event_2_1 = anEvent(streamId_2, 1L, "event_1_stream_2", 3L);
        final Event event_2_2 = anEvent(streamId_2, maxPositionInStream_stream_2, "event_2_stream_12", 4L);
        
        insert(event_1_1);
        insert(event_1_2);
        insert(event_2_1);
        insert(event_2_2);

        final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositions(event_1_1.getId(), batchSize);

        assertThat(streamPositions.size(), is(2));

        assertThat(streamPositions, hasItems(
                new StreamPosition(streamId_1, maxPositionInStream_stream_1),
                new StreamPosition(streamId_2, maxPositionInStream_stream_2)));
    }

    @Test
    public void shouldIgnoreRowsWithNullEventNumbers() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId = fromString("100bdbb4-da77-4f9f-b461-3980e3170001");
        final int batchSize = 100;

        final Event event_1 = anEvent(streamId, 1L, "event_1", 1L);
        final Event event_2 = anEvent(streamId, 2L, "event_2", 2L);
        final Event event_3 = anEvent(streamId, 3L, "event_3", 3L);
        final Event event_4 = anEvent(streamId, 4L, "event_4", null);

        insert(event_1);
        insert(event_2);
        insert(event_3);
        insert(event_4);

        final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositions(event_1.getId(), batchSize);

        assertThat(streamPositions.size(), is(1));

        assertThat(streamPositions.get(0), is(new StreamPosition(streamId, 3L)));
    }

    @Test
    public void shouldLimitResultsByBatchSize() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final int batchSize_1 = 2;
        final int batchSize_2 = 3;

        // create events on different streams
        final Event event_1 = anEvent(fromString("100bdbb4-da77-4f9f-b461-3980e3170001"), 11L, "some-event", 1L);
        final Event event_2 = anEvent(fromString("200bdbb4-da77-4f9f-b461-3980e3170002"), 12L, "some-event", 2L);
        final Event event_3 = anEvent(fromString("300bdbb4-da77-4f9f-b461-3980e3170003"), 13L, "some-event", 3L);
        final Event event_4 = anEvent(fromString("400bdbb4-da77-4f9f-b461-3980e3170004"), 14L, "some-event", 4L);
        final Event event_5 = anEvent(fromString("500bdbb4-da77-4f9f-b461-3980e3170005"), 15L, "some-event", 5L);

        insert(event_1);
        insert(event_2);
        insert(event_3);
        insert(event_4);
        insert(event_5);

        assertThat(eventDiscoveryRepository.getLatestStreamPositions(event_2.getId(), batchSize_1).size(), is(batchSize_1));
        assertThat(eventDiscoveryRepository.getLatestStreamPositions(event_2.getId(), batchSize_2).size(), is(batchSize_2));
    }

    @Test
    public void shouldFindMaxPositionInStreamForAllStreamsGreaterThanEventNumber() throws Exception {
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId_1 = fromString("100bdbb4-da77-4f9f-b461-3980e3170001");
        final UUID streamId_2 = fromString("200bdbb4-da77-4f9f-b461-3980e3170002");
        final long maxPositionInStream_stream_1 = 11L;
        final long maxPositionInStream_stream_2 = 22L;
        final long eventNumberToSearchFrom = 2L;
        final int batchSize = 100;

        final Event event_1_1 = anEvent(streamId_1, 1L, "event_1_stream_1", 1L);
        final Event event_1_2 = anEvent(streamId_1, maxPositionInStream_stream_1, "event_2_stream_2", eventNumberToSearchFrom);
        final Event event_2_1 = anEvent(streamId_2, 1L, "event_1_stream_2", 3L);
        final Event event_2_2 = anEvent(streamId_2, maxPositionInStream_stream_2, "event_2_stream_12", 4L);

        insert(event_1_1);
        insert(event_1_2);
        insert(event_2_1);
        insert(event_2_2);

        final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositions(event_1_2.getId(), batchSize);

        assertThat(streamPositions.size(), is(1));

        assertThat(streamPositions, hasItems(
                new StreamPosition(streamId_2, maxPositionInStream_stream_2)));

    }

    private void insert(final Event event) throws Exception {
        final String sql = """
                INSERT INTO event_log (
                    id,
                    stream_id,
                    position_in_stream,
                    name,
                    event_number,
                    metadata,
                    payload)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                """;


        final Long eventNumber = event.getEventNumber().orElse(null);
        try(final Connection connection = eventStoreDataSource.getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, event.getId());
            preparedStatement.setObject(2, event.getStreamId());
            preparedStatement.setLong(3, event.getPositionInStream());
            preparedStatement.setString(4, event.getName());
            preparedStatement.setObject(5, eventNumber);
            preparedStatement.setString(6, "some-metadata");
            preparedStatement.setString(7, "some-payload");

            preparedStatement.execute();
        }
    }

    private Event anEvent(final UUID streamId, final Long positionInStream, final String name, final Long eventNumber) {
        return new Event(
                randomUUID(),
                streamId,
                positionInStream, name, "some-metadata", "some-payload",
                new UtcClock().now(),
                ofNullable(eventNumber));
    }
}