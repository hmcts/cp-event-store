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
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
public class EventPublishingRepositoryTest {

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

        final Optional<PublishedEvent> publishedEvent = eventPublishingRepository.findEventFromEventLog(eventId_2);

        if (publishedEvent.isPresent()) {
            assertThat(publishedEvent.get().getId(), is(eventId_2));
            assertThat(publishedEvent.get().getStreamId(), is(streamId));
            assertThat(publishedEvent.get().getPositionInStream(), is(2L));
            assertThat(publishedEvent.get().getPayload(), is("some-payload-2"));
            assertThat(publishedEvent.get().getMetadata(), is("some-metadata-2"));
            assertThat(publishedEvent.get().getEventNumber(), is(of(2L)));
            assertThat(publishedEvent.get().getPreviousEventNumber(), is(1L));
        }   else {
            fail();
        }
    }

    @Test
    public void shouldGetNextEventIdFromPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        insertIntoPublishQueue(eventId);

        final Optional<UUID> nextEventIdFromPublishQueue = eventPublishingRepository.getNextEventIdFromPublishQueue();

        if (nextEventIdFromPublishQueue.isPresent()) {
            assertThat(nextEventIdFromPublishQueue.get(), is(eventId));
        } else {
            fail();
        }
    }

    @Test
    public void shouldReturnEmptyIfNoEvenIdsInPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);
        assertThat(eventPublishingRepository.getNextEventIdFromPublishQueue(), is(empty()));
    }

    @Test
    public void shouldDeleteEventIdFromPublishQueue() throws Exception {

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(eventStoreDataSource);

        final UUID eventId = randomUUID();
        insertIntoPublishQueue(eventId);

        final Optional<UUID> nextEventIdFromPublishQueue = eventPublishingRepository.getNextEventIdFromPublishQueue();

        if (nextEventIdFromPublishQueue.isPresent()) {
            assertThat(nextEventIdFromPublishQueue.get(), is(eventId));
        } else {
            fail();
        }

        eventPublishingRepository.removeFromPublishQueue(eventId);

        assertThat(eventPublishingRepository.getNextEventIdFromPublishQueue(), is(empty()));
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

        try(final Connection connection = eventStoreDataSource.getConnection();
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

    private void insertIntoPublishQueue(final UUID eventId) throws Exception {

        final String sql = """
                INSERT INTO publish_queue (event_log_id, date_queued)
                VALUES (?, ?)
                """;
        try(final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, eventId);
            preparedStatement.setTimestamp(2, toSqlTimestamp(new UtcClock().now()));

            preparedStatement.execute();
        }
    }
}