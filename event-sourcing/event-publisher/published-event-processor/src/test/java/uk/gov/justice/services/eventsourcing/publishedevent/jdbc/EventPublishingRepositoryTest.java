package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository.FIND_EVENT_FROM_EVENT_LOG_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository.REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository.UPDATE_IS_PUBLISHED_FLAG_SQL;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventPublishingRepositoryTest {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventPublishingRepository eventPublishingRepository;

    @Test
    public void shouldFindEventInEventLogTable() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long positionInStream = 23L;
        final String name = "some-event-name";
        final String metadata = "some-event-metadata";
        final String payload = "some-event-payload";
        final ZonedDateTime createdAt = new UtcClock().now();
        final long eventNumber = 42L;
        final long previousEventNumber = 41L;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_EVENT_FROM_EVENT_LOG_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);

        when(resultSet.getObject("stream_id", UUID.class)).thenReturn(streamId);
        when(resultSet.getLong("position_in_stream")).thenReturn(positionInStream);
        when(resultSet.getString("name")).thenReturn(name);
        when(resultSet.getString("metadata")).thenReturn(metadata);
        when(resultSet.getString("payload")).thenReturn(payload);
        when(resultSet.getTimestamp("date_created")).thenReturn(toSqlTimestamp(createdAt));
        when(resultSet.getLong("event_number")).thenReturn(eventNumber);
        when(resultSet.getLong("previous_event_number")).thenReturn(previousEventNumber);

        final Optional<LinkedEvent> linkedEvent = eventPublishingRepository.findEventFromEventLog(eventId);

        if (linkedEvent.isPresent()) {
            assertThat(linkedEvent.get().getId(), is(eventId));
            assertThat(linkedEvent.get().getStreamId(), is(streamId));
            assertThat(linkedEvent.get().getPositionInStream(), is(positionInStream));
            assertThat(linkedEvent.get().getName(), is(name));
            assertThat(linkedEvent.get().getMetadata(), is(metadata));
            assertThat(linkedEvent.get().getPayload(), is(payload));
            assertThat(linkedEvent.get().getCreatedAt(), is(createdAt));
            assertThat(linkedEvent.get().getEventNumber(), is(of(eventNumber)));
            assertThat(linkedEvent.get().getPreviousEventNumber(), is(previousEventNumber));
        } else {
            fail();
        }

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).setObject(1, eventId);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).next();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoEventFoundInEventLogTable() throws Exception {

        final UUID eventId = randomUUID();

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_EVENT_FROM_EVENT_LOG_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final Optional<LinkedEvent> linkedEvent = eventPublishingRepository.findEventFromEventLog(eventId);

        assertThat(linkedEvent.isPresent(), is(false));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).setObject(1, eventId);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).next();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfFindingEventInEventLogTableFails() throws Exception {

        final UUID eventId = fromString("c3e788ef-ba19-4024-9535-53c755f17756");
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_EVENT_FROM_EVENT_LOG_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> eventPublishingRepository.findEventFromEventLog(eventId));

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to find event in event_log with id 'c3e788ef-ba19-4024-9535-53c755f17756'"));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).setObject(1, eventId);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldPopNextEventIdFromPublishQueue() throws Exception {

        final UUID eventId = randomUUID();

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, UUID.class)).thenReturn(eventId);

        assertThat(eventPublishingRepository.popNextEventIdFromPublishQueue(), is(of(eventId)));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoNextEventIdFoundInPublishQueue() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(eventPublishingRepository.popNextEventIdFromPublishQueue(), is(empty()));

        final InOrder inOrder = inOrder(preparedStatement, resultSet, connection);

        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfGettingNextEventIdFromPublishQueueFails() throws Exception {

        final UUID eventId = randomUUID();

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final SQLException sqlException = new SQLException("Ooops");

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> eventPublishingRepository.popNextEventIdFromPublishQueue());

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to find next event id from publish_queue table"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldSetIsPublishedFlag() throws Exception {

        final UUID eventId = randomUUID();
        final boolean isPublished = true;
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE_IS_PUBLISHED_FLAG_SQL)).thenReturn(preparedStatement);

        eventPublishingRepository.setIsPublishedFlag(eventId, isPublished);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setBoolean(1, isPublished);
        inOrder.verify(preparedStatement).setObject(2, eventId);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfSettingIsPublishedFlagFails() throws Exception {

        final UUID eventId = fromString("1061c9ae-8f83-4d90-8f46-232127390e6a");
        final boolean isPublished = true;
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final SQLException sqlException = new SQLException("Ooops");

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE_IS_PUBLISHED_FLAG_SQL)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> eventPublishingRepository.setIsPublishedFlag(eventId, isPublished));

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to update 'is_published' on event_log for event id '1061c9ae-8f83-4d90-8f46-232127390e6a'"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setBoolean(1, isPublished);
        inOrder.verify(preparedStatement).setObject(2, eventId);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }
}