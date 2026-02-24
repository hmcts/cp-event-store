package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventStatus.HEALTHY;
import static uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventStatus.PUBLISH_FAILED;
import static uk.gov.justice.services.test.utils.events.LinkedEventBuilder.linkedEventBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.JdbcResultSetStreamer;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.test.utils.persistence.DatabaseCleaner;
import uk.gov.justice.services.test.utils.persistence.FrameworkTestDataSourceFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class MultipleDataSourceLinkedEventRepositoryIT {

    private DataSource dataSource;

    private MultipleDataSourceEventRepository multipleDataSourceEventRepository;

    @BeforeEach
    public void initialize() throws Exception {
        dataSource = new FrameworkTestDataSourceFactory().createEventStoreDataSource();
        new DatabaseCleaner().cleanEventStoreTables("framework");

        final JdbcResultSetStreamer jdbcResultSetStreamer = new JdbcResultSetStreamer();
        final PreparedStatementWrapperFactory preparedStatementWrapperFactory = new PreparedStatementWrapperFactory();
        multipleDataSourceEventRepository = new MultipleDataSourceEventRepository(
                jdbcResultSetStreamer,
                preparedStatementWrapperFactory,
                dataSource);
    }

    @AfterEach
    public void after() throws SQLException {
        dataSource.getConnection().close();
    }

    @Test
    public void shouldGetEventsSinceEventNumber() throws Exception {

        final UUID streamId = randomUUID();

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).withStreamId(streamId).withPositionInStream(2L).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).withStreamId(streamId).withPositionInStream(3L).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).withStreamId(streamId).withPositionInStream(4L).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).withStreamId(streamId).withPositionInStream(5L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventsSince(3)
                .toList();

        assertThat(linkedEvents.size(), is(2));

        assertThat(linkedEvents.get(0).getId(), is(event_4.getId()));
        assertThat(linkedEvents.get(1).getId(), is(event_5.getId()));
    }

    @Test
    public void shouldGetEventRange() throws Exception {

        final UUID streamId = randomUUID();

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).withStreamId(streamId).withPositionInStream(2L).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).withStreamId(streamId).withPositionInStream(3L).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).withStreamId(streamId).withPositionInStream(4L).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).withStreamId(streamId).withPositionInStream(5L).build();
        final LinkedEvent event_6 = linkedEventBuilder().withPreviousEventNumber(5).withEventNumber(6).withStreamId(streamId).withPositionInStream(6L).build();
        final LinkedEvent event_7 = linkedEventBuilder().withPreviousEventNumber(6).withEventNumber(7).withStreamId(streamId).withPositionInStream(7L).build();
        final LinkedEvent event_8 = linkedEventBuilder().withPreviousEventNumber(7).withEventNumber(8).withStreamId(streamId).withPositionInStream(8L).build();
        final LinkedEvent event_9 = linkedEventBuilder().withPreviousEventNumber(8).withEventNumber(9).withStreamId(streamId).withPositionInStream(9L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);
        insertLinkedEvent(event_6, connection);
        insertLinkedEvent(event_7, connection);
        insertLinkedEvent(event_8, connection);
        insertLinkedEvent(event_9, connection);

        updateEventStream(streamId, true, 23L, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventRange(5, 9)
                .toList();

        assertThat(linkedEvents.size(), is(4));

        assertThat(linkedEvents.get(0).getId(), is(event_5.getId()));
        assertThat(linkedEvents.get(0).getEventNumber(), is(of(5L)));
        assertThat(linkedEvents.get(0).getPreviousEventNumber(), is(4L));

        assertThat(linkedEvents.get(1).getId(), is(event_6.getId()));
        assertThat(linkedEvents.get(1).getEventNumber(), is(of(6L)));
        assertThat(linkedEvents.get(1).getPreviousEventNumber(), is(5L));

        assertThat(linkedEvents.get(2).getId(), is(event_7.getId()));
        assertThat(linkedEvents.get(2).getEventNumber(), is(of(7L)));
        assertThat(linkedEvents.get(2).getPreviousEventNumber(), is(6L));

        assertThat(linkedEvents.get(3).getId(), is(event_8.getId()));
        assertThat(linkedEvents.get(3).getEventNumber(), is(of(8L)));
        assertThat(linkedEvents.get(3).getPreviousEventNumber(), is(7L));
    }

    @Test
    public void shouldHandleFirstPreviousEventNumberOfZeroWhenGettingEventRange() throws Exception {

        final UUID streamId = randomUUID();

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).withStreamId(streamId).withPositionInStream(2L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);

        updateEventStream(streamId, true, 23L, connection); 

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventRange(1, 9)
                .toList();

        assertThat(linkedEvents.size(), is(2));

        assertThat(linkedEvents.get(0).getId(), is(event_1.getId()));
        assertThat(linkedEvents.get(0).getEventNumber(), is(of(1L)));
        assertThat(linkedEvents.get(0).getPreviousEventNumber(), is(0L));

        assertThat(linkedEvents.get(1).getId(), is(event_2.getId()));
        assertThat(linkedEvents.get(1).getEventNumber(), is(of(2L)));
        assertThat(linkedEvents.get(1).getPreviousEventNumber(), is(1L));
    }

    @Test
    public void shouldHandleMissingPreviousEventNumbersWheGettingEventRange() throws Exception {

        final UUID streamId = randomUUID();

        // Setting previous event numbers here to avoid null pointers...
        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(2).withStreamId(streamId).withPositionInStream(2L).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(3).withStreamId(streamId).withPositionInStream(3L).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(4).withStreamId(streamId).withPositionInStream(4L).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(5).withStreamId(streamId).withPositionInStream(5L).build();
        final LinkedEvent event_6 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(6).withStreamId(streamId).withPositionInStream(6L).build();
        final LinkedEvent event_7 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(7).withStreamId(streamId).withPositionInStream(7L).build();
        final LinkedEvent event_8 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(8).withStreamId(streamId).withPositionInStream(8L).build();
        final LinkedEvent event_9 = linkedEventBuilder().withPreviousEventNumber(-1L).withEventNumber(9).withStreamId(streamId).withPositionInStream(9L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);
        insertLinkedEvent(event_6, connection);
        insertLinkedEvent(event_7, connection);
        insertLinkedEvent(event_8, connection);
        insertLinkedEvent(event_9, connection);

        updateEventStream(streamId, true, 23L, connection);

        // ...and then deleting the event numbers
        setPreviousEventNumbersToNull(connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventRange(5, 9)
                .toList();

        assertThat(linkedEvents.size(), is(4));

        assertThat(linkedEvents.get(0).getId(), is(event_5.getId()));
        assertThat(linkedEvents.get(0).getEventNumber(), is(of(5L)));
        assertThat(linkedEvents.get(0).getPreviousEventNumber(), is(4L));

        assertThat(linkedEvents.get(1).getId(), is(event_6.getId()));
        assertThat(linkedEvents.get(1).getEventNumber(), is(of(6L)));
        assertThat(linkedEvents.get(1).getPreviousEventNumber(), is(5L));

        assertThat(linkedEvents.get(2).getId(), is(event_7.getId()));
        assertThat(linkedEvents.get(2).getEventNumber(), is(of(7L)));
        assertThat(linkedEvents.get(2).getPreviousEventNumber(), is(6L));

        assertThat(linkedEvents.get(3).getId(), is(event_8.getId()));
        assertThat(linkedEvents.get(3).getEventNumber(), is(of(8L)));
        assertThat(linkedEvents.get(3).getPreviousEventNumber(), is(7L));
    }

    @Test
    public void shouldIgnoreEventsOnInactiveStreamsWheGettingEventRange() throws Exception {

        final UUID streamId = randomUUID();
        final UUID inactiveStreamId = randomUUID();

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).withStreamId(inactiveStreamId).withPositionInStream(2L).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).withStreamId(streamId).withPositionInStream(3L).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).withStreamId(inactiveStreamId).withPositionInStream(4L).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).withStreamId(streamId).withPositionInStream(5L).build();
        final LinkedEvent event_6 = linkedEventBuilder().withPreviousEventNumber(5).withEventNumber(6).withStreamId(inactiveStreamId).withPositionInStream(6L).build();
        final LinkedEvent event_7 = linkedEventBuilder().withPreviousEventNumber(6).withEventNumber(7).withStreamId(streamId).withPositionInStream(7L).build();
        final LinkedEvent event_8 = linkedEventBuilder().withPreviousEventNumber(7).withEventNumber(8).withStreamId(inactiveStreamId).withPositionInStream(8L).build();
        final LinkedEvent event_9 = linkedEventBuilder().withPreviousEventNumber(8).withEventNumber(9).withStreamId(streamId).withPositionInStream(9L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);
        insertLinkedEvent(event_6, connection);
        insertLinkedEvent(event_7, connection);
        insertLinkedEvent(event_8, connection);
        insertLinkedEvent(event_9, connection);

        updateEventStream(streamId, true, 23L, connection);
        updateEventStream(inactiveStreamId, false, 2398L, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventRange(0, 100)
                .toList();

        assertThat(linkedEvents.size(), is(5));

        assertThat(linkedEvents.get(0).getId(), is(event_1.getId()));
        assertThat(linkedEvents.get(0).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(1).getId(), is(event_3.getId()));
        assertThat(linkedEvents.get(1).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(2).getId(), is(event_5.getId()));
        assertThat(linkedEvents.get(2).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(3).getId(), is(event_7.getId()));
        assertThat(linkedEvents.get(3).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(4).getId(), is(event_9.getId()));
        assertThat(linkedEvents.get(4).getStreamId(), is(streamId));
    }

    @Test
    public void shouldIgnoreEventsNotMarkedAsHealthyWheGettingEventRange() throws Exception {

        final UUID streamId = randomUUID();
        final UUID inactiveStreamId = randomUUID();

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).withStreamId(streamId).withPositionInStream(1L).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).withStreamId(streamId).withPositionInStream(2L).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).withStreamId(streamId).withPositionInStream(3L).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).withStreamId(streamId).withPositionInStream(4L).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).withStreamId(streamId).withPositionInStream(5L).build();
        final LinkedEvent event_6 = linkedEventBuilder().withPreviousEventNumber(5).withEventNumber(6).withStreamId(streamId).withPositionInStream(6L).build();
        final LinkedEvent event_7 = linkedEventBuilder().withPreviousEventNumber(6).withEventNumber(7).withStreamId(streamId).withPositionInStream(7L).build();
        final LinkedEvent event_8 = linkedEventBuilder().withPreviousEventNumber(7).withEventNumber(8).withStreamId(streamId).withPositionInStream(8L).build();
        final LinkedEvent event_9 = linkedEventBuilder().withPreviousEventNumber(8).withEventNumber(9).withStreamId(streamId).withPositionInStream(9L).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, HEALTHY, connection);
        insertLinkedEvent(event_2, PUBLISH_FAILED, connection);
        insertLinkedEvent(event_3, HEALTHY, connection);
        insertLinkedEvent(event_4, PUBLISH_FAILED, connection);
        insertLinkedEvent(event_5, HEALTHY, connection);
        insertLinkedEvent(event_6, PUBLISH_FAILED, connection);
        insertLinkedEvent(event_7, HEALTHY, connection);
        insertLinkedEvent(event_8, PUBLISH_FAILED, connection);
        insertLinkedEvent(event_9, HEALTHY, connection);

        updateEventStream(streamId, true, 23L, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository
                .findEventRange(0, 100)
                .toList();

        assertThat(linkedEvents.size(), is(5));

        assertThat(linkedEvents.get(0).getId(), is(event_1.getId()));
        assertThat(linkedEvents.get(0).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(1).getId(), is(event_3.getId()));
        assertThat(linkedEvents.get(1).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(2).getId(), is(event_5.getId()));
        assertThat(linkedEvents.get(2).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(3).getId(), is(event_7.getId()));
        assertThat(linkedEvents.get(3).getStreamId(), is(streamId));
        assertThat(linkedEvents.get(4).getId(), is(event_9.getId()));
        assertThat(linkedEvents.get(4).getStreamId(), is(streamId));
    }

    @Test
    public void fetchByEventIdShouldReturnEventIfExists() throws Exception {
        final Connection connection = dataSource.getConnection();
        final LinkedEvent event = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        insertLinkedEvent(event, connection);

        final Optional<LinkedEvent> fetchedEvent = multipleDataSourceEventRepository.findByEventId(event.getId());

        assertTrue(fetchedEvent.isPresent());
        assertThat(fetchedEvent.get().getId(), is(event.getId()));
    }

    @Test
    public void fetchByEventIdShouldReturnEmptyIfEventNotExist() throws Exception {
        final Optional<LinkedEvent> fetchedEvent = multipleDataSourceEventRepository.findByEventId(randomUUID());

        assertFalse(fetchedEvent.isPresent());
    }

    @Test
    public void shouldGetLatestEvent() throws Exception {

        final LinkedEvent event_1 = linkedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        final LinkedEvent event_2 = linkedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).build();
        final LinkedEvent event_3 = linkedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).build();
        final LinkedEvent event_4 = linkedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).build();
        final LinkedEvent event_5 = linkedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);

        final Optional<LinkedEvent> latestPublishedEvent = multipleDataSourceEventRepository.getLatestLinkedEvent();

        if (latestPublishedEvent.isPresent()) {
            assertThat(latestPublishedEvent.get().getId(), is(event_5.getId()));
            assertThat(latestPublishedEvent.get().getName(), is(event_5.getName()));
            assertThat(latestPublishedEvent.get().getStreamId(), is(event_5.getStreamId()));
            assertThat(latestPublishedEvent.get().getMetadata(), is(event_5.getMetadata()));
            assertThat(latestPublishedEvent.get().getCreatedAt(), is(event_5.getCreatedAt()));
            assertThat(latestPublishedEvent.get().getEventNumber(), is(event_5.getEventNumber()));
            assertThat(latestPublishedEvent.get().getPreviousEventNumber(), is(event_5.getPreviousEventNumber()));
            assertThat(latestPublishedEvent.get().getPositionInStream(), is(event_5.getPositionInStream()));
            assertThat(latestPublishedEvent.get().getPayload(), is(event_5.getPayload()));
        } else {
            fail();
        }
    }

    @Test
    public void shouldReturnEmptyWhenGettingLatestPublishedEventIfNoPublishedEventsExist() throws Exception {
        assertThat(multipleDataSourceEventRepository.getLatestLinkedEvent(), is(empty()));
    }

    private void insertLinkedEvent(final LinkedEvent linkedEvent, final Connection connection) throws SQLException {

        insertLinkedEvent(linkedEvent, HEALTHY, connection);
    }

    private void insertLinkedEvent(final LinkedEvent linkedEvent, final EventStatus eventStatus, final Connection connection) throws SQLException {

        final String sql = """
            INSERT into event_log (
                id,
                stream_id,
                position_in_stream,
                name,
                payload,
                metadata,
                date_created,
                event_status,
                event_number,
                previous_event_number)
            VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, linkedEvent.getId());
            preparedStatement.setObject(2, linkedEvent.getStreamId());
            preparedStatement.setLong(3, linkedEvent.getPositionInStream());
            preparedStatement.setString(4, linkedEvent.getName());
            preparedStatement.setString(5, linkedEvent.getPayload());
            preparedStatement.setString(6, linkedEvent.getMetadata());
            preparedStatement.setObject(7, toSqlTimestamp(linkedEvent.getCreatedAt()));
            preparedStatement.setString(8, eventStatus.toString());
            preparedStatement.setLong(9, linkedEvent.getEventNumber().orElseThrow(() -> new MissingEventNumberException("Event with id '%s' does not have an event number")));
            preparedStatement.setLong(10, linkedEvent.getPreviousEventNumber());

            preparedStatement.execute();
        }
    }

    private void setPreviousEventNumbersToNull(final Connection connection) throws SQLException {

        final String sql = """
                UPDATE event_log
                SET previous_event_number = NULL
                """;
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        }
    }

    private void updateEventStream(
            final UUID streamId,
            final boolean active,
            final Long positionInStream,
            final Connection connection) throws SQLException {

        final String sql = """
                INSERT INTO event_stream(stream_id, position_in_stream, active, date_created)
                VALUES (?, ?, ?, ?)
                """;
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, streamId);
            preparedStatement.setLong(2, positionInStream);
            preparedStatement.setBoolean(3, active);
            preparedStatement.setTimestamp(4, toSqlTimestamp(new UtcClock().now()));
            preparedStatement.executeUpdate();
        }


    }
}