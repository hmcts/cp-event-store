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
import java.util.Optional;
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
    public void shouldFindMaxPositionInStreamForAllStreamsBetweenEventNumbers() throws Exception {

        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId_1 = fromString("100bdbb4-da77-4f9f-b461-3980e3170001");
        final UUID streamId_2 = fromString("200bdbb4-da77-4f9f-b461-3980e3170002");
        final long maxPositionInStream_stream_1 = 11L;
        final long maxPositionInStream_stream_2 = 22L;

        final Event event_1_1 = anEvent(streamId_1, 1L, "event_1_stream_1", 1L);
        final Event event_1_2 = anEvent(streamId_1, maxPositionInStream_stream_1, "event_2_stream_2", 2L);
        final Event event_2_1 = anEvent(streamId_2, 1L, "event_1_stream_2", 3L);
        final Event event_2_2 = anEvent(streamId_2, maxPositionInStream_stream_2, "event_2_stream_12", 4L);

        insert(event_1_1);
        insert(event_1_2);
        insert(event_2_1);
        insert(event_2_2);

        final List<StreamPosition> streamPositions = eventDiscoveryRepository.getLatestStreamPositionsBetween(1L, 4L);

        assertThat(streamPositions.size(), is(2));

        assertThat(streamPositions, hasItems(
                new StreamPosition(streamId_1, maxPositionInStream_stream_1),
                new StreamPosition(streamId_2, maxPositionInStream_stream_2)));
    }

    @Test
    public void shouldGetEventNumberForEventId() throws Exception {
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId = randomUUID();
        final Event event = anEvent(streamId, 1L, "event-1", 123L);
        insert(event);

        final Long eventNumber = eventDiscoveryRepository.getEventNumberFor(event.getId());

        assertThat(eventNumber, is(123L));
    }

    @Test
    public void shouldGetLatestEventIdAndNumberAtOffset() throws Exception {
        final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID streamId = randomUUID();
        final Event event_1 = anEvent(streamId, 1L, "event-1", 100L);
        final Event event_2 = anEvent(streamId, 2L, "event-2", 105L);
        final Event event_3 = anEvent(streamId, 3L, "event-3", 115L);

        insert(event_1);
        insert(event_2);
        insert(event_3);

        // lastEventNumber = 100, batchSize = 10 -> look for event_number <= 110. Should return event_2 (105).
        final Optional<EventIdNumber> result_1 = eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(100L, 10);
        assertThat(result_1.isPresent(), is(true));
        assertThat(result_1.get().id(), is(event_2.getId()));
        assertThat(result_1.get().eventNumber(), is(105L));

        // lastEventNumber = 100, batchSize = 20 -> look for event_number <= 120. Should return event_3 (115).
        final Optional<EventIdNumber> result_2 = eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(100L, 20);
        assertThat(result_2.isPresent(), is(true));
        assertThat(result_2.get().id(), is(event_3.getId()));
        assertThat(result_2.get().eventNumber(), is(115L));

        // lastEventNumber = 120, batchSize = 10 -> look for event_number <= 130. Should return event_3 (115).
        final Optional<EventIdNumber> result_3 = eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(120L, 10);
        assertThat(result_3.isPresent(), is(true));
        assertThat(result_3.get().id(), is(event_3.getId()));
        assertThat(result_3.get().eventNumber(), is(115L));
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