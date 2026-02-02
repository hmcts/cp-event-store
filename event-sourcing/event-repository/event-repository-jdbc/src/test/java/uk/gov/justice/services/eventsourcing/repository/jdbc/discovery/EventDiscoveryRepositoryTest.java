package uk.gov.justice.services.eventsourcing.repository.jdbc.discovery;

import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.repository.jdbc.discovery.EventDiscoveryRepository.GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_BETWEEN_EVENT_NUMBERS_SQL;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
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
public class EventDiscoveryRepositoryTest {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private EventDiscoveryRepository eventDiscoveryRepository;

    @Test
    public void shouldGetTheLatestStreamPositionsBetweenEventNumbers() throws Exception {

        final UUID streamId_1 = randomUUID();
        final UUID streamId_2 = randomUUID();
        final Long positionInStream_1 = 11L;
        final Long positionInStream_2 = 22L;
        final long firstEventNumber = 10L;
        final long lastEventNumber = 20L;

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(connection.prepareStatement(GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_BETWEEN_EVENT_NUMBERS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getObject("stream_id", UUID.class)).thenReturn(streamId_1, streamId_2);
        when(resultSet.getObject("max_position_in_stream", Long.class)).thenReturn(positionInStream_1, positionInStream_2);

        final List<StreamPosition> latestStreamPositions = eventDiscoveryRepository.getLatestStreamPositionsBetween(
                firstEventNumber,
                lastEventNumber);

        assertThat(latestStreamPositions.size(), is(2));
        assertThat(latestStreamPositions.get(0).streamId(), is(streamId_1));
        assertThat(latestStreamPositions.get(0).positionInStream(), is(positionInStream_1));
        assertThat(latestStreamPositions.get(1).streamId(), is(streamId_2));
        assertThat(latestStreamPositions.get(1).positionInStream(), is(positionInStream_2));

        final InOrder inOrder = inOrder(connection, preparedStatement, resultSet);
        inOrder.verify(preparedStatement).setLong(1, firstEventNumber);
        inOrder.verify(preparedStatement).setLong(2, lastEventNumber);
        inOrder.verify(preparedStatement).executeQuery();
        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventStoreEventDiscoveryExceptionIfGettingTheLatestStreamPositionsForStreamsFails() throws Exception {

        final long firstEventNumber = 10L;
        final long lastEventNumber = 20L;
        final SQLException sqlException = new SQLException("Ooops");

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(connection.prepareStatement(GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_BETWEEN_EVENT_NUMBERS_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final EventStoreEventDiscoveryException eventStoreEventDiscoveryException = assertThrows(
                EventStoreEventDiscoveryException.class,
                () -> eventDiscoveryRepository.getLatestStreamPositionsBetween(firstEventNumber, lastEventNumber));

        assertThat(eventStoreEventDiscoveryException.getCause(), is(sqlException));
        assertThat(eventStoreEventDiscoveryException.getMessage(), is("Failed to get latest stream positions between eventNumbers '10' and '20'"));

        final InOrder inOrder = inOrder(connection, preparedStatement);
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldGetEventNumberForEventId() throws Exception {
        final UUID eventId = randomUUID();
        final Long eventNumber = 123L;

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(connection.prepareStatement("SELECT event_number FROM event_log WHERE id = ?")).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong("event_number")).thenReturn(eventNumber);

        final Long result = eventDiscoveryRepository.getEventNumberFor(eventId);

        assertThat(result, is(eventNumber));
        verify(preparedStatement).setObject(1, eventId);
    }

    @Test
    public void shouldGetLatestEventIdAndNumberAtOffset() throws Exception {
        final long lastEventNumber = 100L;
        final int batchSize = 10;
        final UUID eventId = randomUUID();
        final long eventNumber = 105L;

        final Connection connection = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(connection.prepareStatement("""
                    SELECT id, event_number
                    FROM event_log
                    WHERE event_number <= ?
                    ORDER BY event_number DESC
                    LIMIT 1
            """)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject("id", UUID.class)).thenReturn(eventId);
        when(resultSet.getLong("event_number")).thenReturn(eventNumber);

        final Optional<EventIdNumber> result = eventDiscoveryRepository.getLatestEventIdAndNumberAtOffset(lastEventNumber, batchSize);

        assertThat(result.isPresent(), is(true));
        assertThat(result.get().id(), is(eventId));
        assertThat(result.get().eventNumber(), is(eventNumber));
        verify(preparedStatement).setLong(1, lastEventNumber + batchSize);
    }
}