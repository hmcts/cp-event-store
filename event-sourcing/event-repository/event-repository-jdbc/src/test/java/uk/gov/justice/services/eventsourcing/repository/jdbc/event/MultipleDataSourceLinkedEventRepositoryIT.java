package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.test.utils.events.LinkedEventBuilder.publishedEventBuilder;

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

        final LinkedEvent event_1 = publishedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        final LinkedEvent event_2 = publishedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).build();
        final LinkedEvent event_3 = publishedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).build();
        final LinkedEvent event_4 = publishedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).build();
        final LinkedEvent event_5 = publishedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository.findEventsSince(3)
                .collect(toList());

        assertThat(linkedEvents.size(), is(2));

        assertThat(linkedEvents.get(0).getId(), is(event_4.getId()));
        assertThat(linkedEvents.get(1).getId(), is(event_5.getId()));
    }

    @Test
    public void shouldGetEventRange() throws Exception {

        final LinkedEvent event_1 = publishedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        final LinkedEvent event_2 = publishedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).build();
        final LinkedEvent event_3 = publishedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).build();
        final LinkedEvent event_4 = publishedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).build();
        final LinkedEvent event_5 = publishedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).build();

        final Connection connection = dataSource.getConnection();

        insertLinkedEvent(event_1, connection);
        insertLinkedEvent(event_2, connection);
        insertLinkedEvent(event_3, connection);
        insertLinkedEvent(event_4, connection);
        insertLinkedEvent(event_5, connection);

        final List<LinkedEvent> linkedEvents = multipleDataSourceEventRepository.findEventRange(1, 4)
                .collect(toList());

        assertThat(linkedEvents.size(), is(3));

        assertThat(linkedEvents.get(0).getId(), is(event_1.getId()));
        assertThat(linkedEvents.get(1).getId(), is(event_2.getId()));
        assertThat(linkedEvents.get(2).getId(), is(event_3.getId()));
    }

    @Test
    public void fetchByEventIdShouldReturnEventIfExists() throws Exception {
        final Connection connection = dataSource.getConnection();
        final LinkedEvent event = publishedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        insertLinkedEvent(event, connection);

        final Optional<LinkedEvent> fetchedEvent = multipleDataSourceEventRepository.findByEventId(event.getId());

        assertTrue(fetchedEvent.isPresent());
        assertThat(fetchedEvent.get().getId(), is(event.getId()));
    }

    @Test
    public void fetchByEventIdShouldReturnEmptyIfEventNotExist() throws Exception {
        final Optional<LinkedEvent> fetchedEvent = multipleDataSourceEventRepository.findByEventId(UUID.randomUUID());

        assertFalse(fetchedEvent.isPresent());
    }

    @Test
    public void shouldGetLatestEvent() throws Exception {

        final LinkedEvent event_1 = publishedEventBuilder().withPreviousEventNumber(0).withEventNumber(1).build();
        final LinkedEvent event_2 = publishedEventBuilder().withPreviousEventNumber(1).withEventNumber(2).build();
        final LinkedEvent event_3 = publishedEventBuilder().withPreviousEventNumber(2).withEventNumber(3).build();
        final LinkedEvent event_4 = publishedEventBuilder().withPreviousEventNumber(3).withEventNumber(4).build();
        final LinkedEvent event_5 = publishedEventBuilder().withPreviousEventNumber(4).withEventNumber(5).build();

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

        final String sql = "INSERT into event_log (" +
                "id, stream_id, position_in_stream, name, payload, metadata, date_created, event_number, previous_event_number) " +
                "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, linkedEvent.getId());
            preparedStatement.setObject(2, linkedEvent.getStreamId());
            preparedStatement.setLong(3, linkedEvent.getPositionInStream());
            preparedStatement.setString(4, linkedEvent.getName());
            preparedStatement.setString(5, linkedEvent.getPayload());
            preparedStatement.setString(6, linkedEvent.getMetadata());
            preparedStatement.setObject(7, toSqlTimestamp(linkedEvent.getCreatedAt()));
            preparedStatement.setLong(8, linkedEvent.getEventNumber().orElseThrow(() -> new MissingEventNumberException("Event with id '%s' does not have an event number")));
            preparedStatement.setLong(9, linkedEvent.getPreviousEventNumber());

            preparedStatement.execute();
        }
    }
}
