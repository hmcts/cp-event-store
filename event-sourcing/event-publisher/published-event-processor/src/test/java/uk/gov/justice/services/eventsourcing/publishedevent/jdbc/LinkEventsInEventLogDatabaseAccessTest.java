package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.SELECT_BATCH_OF_UNLINKED_EVENTS;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess.UPDATE_EVENT_NUMBERS_FOR_EVENT;

import org.slf4j.Logger;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import javax.sql.DataSource;

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

    @Mock
    private Logger logger;

    @InjectMocks
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Test
    public void shouldGetEventStoreConnection() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);

        assertThat(linkEventsInEventLogDatabaseAccess.getEventStoreConnection(), is(connection));
    }

    @Test
    public void shouldFindCurrentHighestEventNumberUsingProvidedConnection() throws Exception {

        final Long highestEventNumber = 42L;
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(highestEventNumber);

        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection), is(highestEventNumber));
    }

    @Test
    public void shouldReturnZeroIfNoEventsFound() throws Exception {

        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(connection.prepareStatement(SELECT_HIGHEST_LINKED_EVENT_NUMBER_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection), is(0L));
    }

    @Test
    public void shouldFindBatchOfNextEventIdsToLink() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        final UUID streamId1 = randomUUID();
        final UUID streamId2 = randomUUID();

        when(connection.prepareStatement(SELECT_BATCH_OF_UNLINKED_EVENTS)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getObject("id", UUID.class)).thenReturn(eventId1, eventId2);
        when(resultSet.getObject("stream_id", UUID.class)).thenReturn(streamId1, streamId2);
        when(resultSet.getLong("position_in_stream")).thenReturn(1L, 2L);

        final List<EventDetailsToLink> result = linkEventsInEventLogDatabaseAccess.findBatchOfNextEventsToLink(connection, 10);

        assertThat(result.size(), is(2));
        assertThat(result.get(0).eventId(), is(eventId1));
        assertThat(result.get(1).eventId(), is(eventId2));
        verify(preparedStatement).setInt(1, 10);
    }

    @Test
    public void shouldLinkEventsBatchUsingJdbcBatch() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(UPDATE_EVENT_NUMBERS_FOR_EVENT)).thenReturn(preparedStatement);

        final List<LinkedEventData> linkDataList = List.of(
                new LinkedEventData(eventId1, 1L, 0L),
                new LinkedEventData(eventId2, 2L, 1L)
        );

        linkEventsInEventLogDatabaseAccess.linkEventsBatch(connection, linkDataList);

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setLong(1, 1L);
        inOrder.verify(preparedStatement).setLong(2, 0L);
        inOrder.verify(preparedStatement).setObject(3, eventId1);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).setLong(1, 2L);
        inOrder.verify(preparedStatement).setLong(2, 1L);
        inOrder.verify(preparedStatement).setObject(3, eventId2);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).executeBatch();
    }

    @Test
    public void shouldInsertBatchIntoPublishQueue() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final ZonedDateTime now = new UtcClock().now();
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(connection.prepareStatement(INSERT_EVENT_INTO_PUBLISH_QUEUE_QUERY)).thenReturn(preparedStatement);
        when(clock.now()).thenReturn(now);

        linkEventsInEventLogDatabaseAccess.insertBatchIntoPublishQueue(connection, List.of(eventId1, eventId2));

        final InOrder inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setObject(1, eventId1);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).setObject(1, eventId2);
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).executeBatch();
    }
}