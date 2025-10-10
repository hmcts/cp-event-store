package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
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
public class EventPublishingRepositoryIT {

    private final DataSource eventStoreDataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventPublishingRepository eventPublishingRepository;

    @BeforeEach
    public void initDatabase() throws Exception {
        new DatabaseCleaner().cleanEventStoreTables("framework");
    }

    @Test
    public void shouldGetLinkedEventFromEventLogTable() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId_1 = randomUUID();
        final UUID eventId_2 = randomUUID();
        final UUID eventId_3 = randomUUID();

        final UUID streamId = randomUUID();

        insertEvent(eventId_1, streamId, 1);
        insertEvent(eventId_2, streamId, 2);
        insertEvent(eventId_3, streamId, 3);

        final Optional<LinkedEvent> publishedEvent = eventPublishingRepository.findEventFromEventLog(eventId_2);

        if (publishedEvent.isPresent()) {
            assertThat(publishedEvent.get().getId(), is(eventId_2));
            assertThat(publishedEvent.get().getStreamId(), is(streamId));
            assertThat(publishedEvent.get().getPositionInStream(), is(2L));
            assertThat(publishedEvent.get().getPayload(), is("some-payload-2"));
            assertThat(publishedEvent.get().getMetadata(), is("some-metadata-2"));
            assertThat(publishedEvent.get().getEventNumber(), is(of(2L)));
            assertThat(publishedEvent.get().getPreviousEventNumber(), is(1L));
        } else {
            fail();
        }
    }

    @Test
    public void shouldGetNextEventIdFromPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        final ZonedDateTime dateCreated_1 = new UtcClock().now().minusSeconds(60);
        final ZonedDateTime dateCreated_2 = new UtcClock().now().minusSeconds(30);
        final ZonedDateTime dateCreated_3 = new UtcClock().now().minusSeconds(15);
        final UUID eventId_1 = randomUUID();
        final UUID eventId_2 = randomUUID();
        final UUID eventId_3 = randomUUID();

        insertIntoPublishQueue(eventId_1, dateCreated_1);
        insertIntoPublishQueue(eventId_2, dateCreated_2);
        insertIntoPublishQueue(eventId_3, dateCreated_3);

        final Optional<UUID> nextEventIdFromPublishQueue_1 = eventPublishingRepository.getNextEventIdFromPublishQueue();
        assertThat(nextEventIdFromPublishQueue_1, is(of(eventId_1)));
        eventPublishingRepository.removeFromPublishQueue(eventId_1);

        final Optional<UUID> nextEventIdFromPublishQueue_2 = eventPublishingRepository.getNextEventIdFromPublishQueue();
        assertThat(nextEventIdFromPublishQueue_2, is(of(eventId_2)));
        eventPublishingRepository.removeFromPublishQueue(eventId_2);

        final Optional<UUID> nextEventIdFromPublishQueue_3 = eventPublishingRepository.getNextEventIdFromPublishQueue();
        assertThat(nextEventIdFromPublishQueue_3, is(of(eventId_3)));
        eventPublishingRepository.removeFromPublishQueue(eventId_3);
    }

    @Test
    public void shouldReturnEmptyIfNoEvenIdsInPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        assertThat(eventPublishingRepository.getNextEventIdFromPublishQueue(), is(empty()));
    }

    @Test
    public void shouldDeleteEventIdFromPublishQueue() throws Exception {

        final ZonedDateTime dateCreated = new UtcClock().now();
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        insertIntoPublishQueue(eventId, dateCreated);

        final Optional<UUID> nextEventIdFromPublishQueue = eventPublishingRepository.getNextEventIdFromPublishQueue();

        if (nextEventIdFromPublishQueue.isPresent()) {
            assertThat(nextEventIdFromPublishQueue.get(), is(eventId));
        } else {
            fail();
        }

        eventPublishingRepository.removeFromPublishQueue(eventId);

        assertThat(eventPublishingRepository.getNextEventIdFromPublishQueue(), is(empty()));
    }

    @Test
    public void shouldSetIsPublishedFlag() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        insertEvent(eventId, streamId, 1);

        // is_published false by default
        assertThat(getIsPublishedFlag(eventId), is(of(false)));

        eventPublishingRepository.setIsPublishedFlag(eventId, true);
        assertThat(getIsPublishedFlag(eventId), is(of(true)));

        eventPublishingRepository.setIsPublishedFlag(eventId, false);
        assertThat(getIsPublishedFlag(eventId), is(of(false)));
    }

    private void insertEvent(final UUID eventId, final UUID streamId, final int eventNumber) throws Exception {

        final String sql = """
                INSERT INTO event_log (
                    id,
                    stream_id,
                    position_in_stream,
                    name,
                    payload,
                    metadata,
                    date_created,
                    event_number,
                    previous_event_number)
                VALUES (?, ?, ?, ?,  ?, ?, ?, ?, ?)
                """;

        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, eventId);
            preparedStatement.setObject(2, streamId);
            preparedStatement.setLong(3, eventNumber);
            preparedStatement.setString(4, "some-name-" + eventNumber);
            preparedStatement.setString(5, "some-payload-" + eventNumber);
            preparedStatement.setString(6, "some-metadata-" + eventNumber);
            preparedStatement.setTimestamp(7, toSqlTimestamp(new UtcClock().now()));
            preparedStatement.setLong(8, eventNumber);
            preparedStatement.setLong(9, eventNumber - 1);

            preparedStatement.execute();
        }
    }

    private void insertIntoPublishQueue(final UUID eventId, final ZonedDateTime dateCreated) throws Exception {

        final String sql = """
                INSERT INTO publish_queue (event_log_id, date_queued)
                VALUES (?, ?)
                """;
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, eventId);
            preparedStatement.setTimestamp(2, toSqlTimestamp(new UtcClock().now()));

            preparedStatement.execute();
        }
    }

    private Optional<Boolean> getIsPublishedFlag(final UUID eventId) throws Exception {

        final String sql = """
                    SELECT is_published
                    FROM event_log
                    WHERE id = ?
                """;
        try (final Connection connection = eventStoreDataSource.getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, eventId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return of(resultSet.getBoolean("is_published"));
                }
            }
        }

        return empty();
    }
}