package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;
import static uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository.FIND_ALL_SQL;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
public class CompatibilityModePublishedEventRepositoryTest {

    @Mock
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @InjectMocks
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @Test
    public void shouldFindAllEventsInEventLogTable() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final Long positionInStream = 983724L;
        final String name = "some-event-name";
        final String payloadJson = "some-event-payload-json";
        final String metadataJson = "some-event-metadata-json";
        final ZonedDateTime createdAt = new UtcClock().now();
        final Timestamp createdAtTimestamp = toSqlTimestamp(createdAt);
        final Long eventNumber = 23L;
        final Long previousEventNumber = 22L;

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);


        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);

        when(resultSet.getObject("id", UUID.class)).thenReturn(eventId);
        when(resultSet.getObject("stream_id", UUID.class)).thenReturn(streamId);
        when(resultSet.getLong("position_in_stream")).thenReturn(positionInStream);
        when(resultSet.getString("name")).thenReturn(name);
        when(resultSet.getString("payload")).thenReturn(payloadJson);
        when(resultSet.getString("metadata")).thenReturn(metadataJson);
        when(resultSet.getTimestamp("date_created")).thenReturn(createdAtTimestamp);
        when(resultSet.getLong("event_number")).thenReturn(eventNumber);
        when(resultSet.getLong("previous_event_number")).thenReturn(previousEventNumber);

        final List<LinkedEvent> allEvents = compatibilityModePublishedEventRepository.findAll();

        assertThat(allEvents.size(), is(1));
        assertThat(allEvents.get(0).getId(), is(eventId));
        assertThat(allEvents.get(0).getStreamId(), is(streamId));
        assertThat(allEvents.get(0).getPositionInStream(), is(positionInStream));
        assertThat(allEvents.get(0).getName(), is(name));
        assertThat(allEvents.get(0).getPayload(), is(payloadJson));
        assertThat(allEvents.get(0).getMetadata(), is(metadataJson));
        assertThat(allEvents.get(0).getCreatedAt(), is(createdAt));
        assertThat(allEvents.get(0).getEventNumber(), is(of(eventNumber)));
        assertThat(allEvents.get(0).getPreviousEventNumber(), is(previousEventNumber));

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);

        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfFindingAllEventsInEventLogTableFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(eventStoreDataSourceProvider.getDefaultDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);

        when(resultSet.getObject("id", UUID.class)).thenThrow(sqlException);

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> compatibilityModePublishedEventRepository.findAll());

        assertThat(eventPublishingException.getCause(), is(sqlException));
        assertThat(eventPublishingException.getMessage(), is("Failed to find all events in event_log table"));

        final InOrder inOrder = inOrder(resultSet, preparedStatement, connection);

        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }
}