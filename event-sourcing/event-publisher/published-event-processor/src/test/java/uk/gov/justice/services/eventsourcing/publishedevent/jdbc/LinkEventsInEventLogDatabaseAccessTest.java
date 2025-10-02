package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.SELECT_NEXT_UNLINKED_EVENT_ID;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.UPDATE_EVENT_NUMBERS_FOR_EVENT;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkEventsInEventLogDatabaseAccessTest {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Mock
    private UtcClock clock;

    @InjectMocks
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Test
    public void shouldAddEventNumbersToEventToLink() throws Exception {

        final UUID eventId = randomUUID();
        final Long eventNumber = 42L;
        final Long previousEventNumber = 41L;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE_EVENT_NUMBERS_FOR_EVENT)).thenReturn(preparedStatement);

        linkEventsInEventLogDatabaseAccess.linkEvent(eventId, eventNumber, previousEventNumber);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setLong(1, eventNumber);
        inOrder.verify(preparedStatement).setLong(2, previousEventNumber);
        inOrder.verify(preparedStatement).setObject(3, eventId);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfAddingEventNumbersToEventFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final UUID eventId = fromString("717cb318-ee4f-4959-ad11-5eff7aca88b4");
        final Long eventNumber = 42L;
        final Long previousEventNumber = 41L;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPDATE_EVENT_NUMBERS_FOR_EVENT)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkEventsInEventLogDatabaseAccess.linkEvent(eventId, eventNumber, previousEventNumber));

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to link event in event_log table. eventId '717cb318-ee4f-4959-ad11-5eff7aca88b4' eventNumber 42, previousEventNumber 41"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setLong(1, eventNumber);
        inOrder.verify(preparedStatement).setLong(2, previousEventNumber);
        inOrder.verify(preparedStatement).setObject(3, eventId);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldGetHighestEventNumberFromEventLogTable() throws Exception {

        final Long highestEventNumber = 42L;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(highestEventNumber);

        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(), is(highestEventNumber));

        final InOrder inOrder = inOrder(preparedStatement, connection);
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnHighestEventNumberOfZeroIfNoEventsFoundInEventLogTable() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(), is(0L));

        final InOrder inOrder = inOrder(preparedStatement, connection);
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfGettingHighestEventNumberFromEventLogTableFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(sqlException);

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable());

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to find highest event number in event log table"));

        final InOrder inOrder = inOrder(preparedStatement, connection);
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldFindIdOfNextEventToLink() throws Exception {

        final UUID eventId = randomUUID();
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_NEXT_UNLINKED_EVENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, UUID.class)).thenReturn(eventId);

        assertThat(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink(), is(of(eventId)));

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoEventsToLinkFound() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_NEXT_UNLINKED_EVENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink(), is(empty()));

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfFindingIdOfNextEventToLinkFails() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);
        final SQLException sqlException = new SQLException("Ooops");

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(SELECT_NEXT_UNLINKED_EVENT_ID)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, UUID.class)).thenThrow(sqlException);

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink());

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed find event id to link"));

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldInsertLinkedEventIntoPublishQueue() throws Exception {

        final UUID eventId = randomUUID();
        final ZonedDateTime now = new UtcClock().now();
        final Timestamp nowAsTimestamp = toSqlTimestamp(now);

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)).thenReturn(preparedStatement);
        when(clock.now()).thenReturn(now);

        linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId);

        final InOrder inOrder = inOrder(preparedStatement, connection);
        inOrder.verify(preparedStatement).setObject(1, eventId);
        inOrder.verify(preparedStatement).setTimestamp(2, nowAsTimestamp);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfInsertingLinkedEventIntoPublishQueueFails() throws Exception {

        final UUID eventId = fromString("ca7e3fb7-e9d4-49f7-b4b4-700556ed0347");
        final ZonedDateTime now = new UtcClock().now();
        final SQLException sqlException = new SQLException("Oops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)).thenReturn(preparedStatement);
        when(clock.now()).thenReturn(now);

        doThrow(sqlException).when(preparedStatement).executeUpdate();

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId));

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to insert linked event into publish_queue table. eventId: 'ca7e3fb7-e9d4-49f7-b4b4-700556ed0347'"));

        final InOrder inOrder = inOrder(preparedStatement, connection);
        inOrder.verify(preparedStatement).executeUpdate();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }
}
