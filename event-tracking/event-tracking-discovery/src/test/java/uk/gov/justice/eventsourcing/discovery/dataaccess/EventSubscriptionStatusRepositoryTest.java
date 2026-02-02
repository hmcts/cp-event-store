package uk.gov.justice.eventsourcing.discovery.dataaccess;

import static java.time.ZoneOffset.UTC;
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
import static uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository.FIND_ALL_SQL;
import static uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository.FIND_BY_SOURCE_AND_COMPONENT_SQL;
import static uk.gov.justice.eventsourcing.discovery.dataaccess.EventSubscriptionStatusRepository.UPSERT_EVENT_SUBSCRIPTION_STATUS_SQL;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.eventsourcing.discovery.EventDiscoveryException;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
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
public class EventSubscriptionStatusRepositoryTest {

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @InjectMocks
    private EventSubscriptionStatusRepository eventSubscriptionStatusRepository;

    @Test
    public void shouldSaveEventSubscriptionStatus() throws Exception {

        final UUID latestEventId = randomUUID();
        final EventSubscriptionStatus eventSubscriptionStatus = new EventSubscriptionStatus(
                "some-source",
                "some-component",
                of(latestEventId),
                new UtcClock().now()
        );

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPSERT_EVENT_SUBSCRIPTION_STATUS_SQL)).thenReturn(preparedStatement);

        eventSubscriptionStatusRepository.save(eventSubscriptionStatus);

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setString(1, eventSubscriptionStatus.source());
        inOrder.verify(preparedStatement).setString(2, eventSubscriptionStatus.component());
        inOrder.verify(preparedStatement).setObject(3, latestEventId);
        inOrder.verify(preparedStatement).setTimestamp(4, toSqlTimestamp(eventSubscriptionStatus.updatedAt()));
        inOrder.verify(preparedStatement).execute();

        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventDiscoveryExceptionIfSavingEventSubscriptionStatusFails() throws Exception {

        final ZonedDateTime updatedAt = ZonedDateTime.of(2026, 1, 2, 11, 22, 46, 0, UTC);
        final UUID latestEventId = fromString("5246fb9c-ecde-4d1c-9cc4-09ec520ff1c3");
        final EventSubscriptionStatus eventSubscriptionStatus = new EventSubscriptionStatus(
                "some-source",
                "some-component",
                of(latestEventId),
                updatedAt
        );
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(UPSERT_EVENT_SUBSCRIPTION_STATUS_SQL)).thenReturn(preparedStatement);
        doThrow(sqlException).when(preparedStatement).execute();

        final EventDiscoveryException eventDiscoveryException = assertThrows(
                EventDiscoveryException.class,
                () -> eventSubscriptionStatusRepository.save(eventSubscriptionStatus));

        assertThat(eventDiscoveryException.getCause(), is(sqlException));
        assertThat(eventDiscoveryException.getMessage(), is("Failed to upsert EventSubscriptionStatus[source=some-source, component=some-component, latestEventId=Optional[5246fb9c-ecde-4d1c-9cc4-09ec520ff1c3], updatedAt=2026-01-02T11:22:46Z]"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setString(1, eventSubscriptionStatus.source());
        inOrder.verify(preparedStatement).setString(2, eventSubscriptionStatus.component());
        inOrder.verify(preparedStatement).setObject(3, latestEventId);
        inOrder.verify(preparedStatement).setTimestamp(4, toSqlTimestamp(eventSubscriptionStatus.updatedAt()));
        inOrder.verify(preparedStatement).execute();

        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldFindBySourceAndComponent() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final UUID latestEventId = randomUUID();
        final ZonedDateTime updatedAt = new UtcClock().now();
        final Timestamp updatedAtTimestamp = toSqlTimestamp(updatedAt);

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SOURCE_AND_COMPONENT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject("latest_event_id", UUID.class)).thenReturn(latestEventId);
        when(resultSet.getTimestamp("updated_at")).thenReturn(updatedAtTimestamp);

        final Optional<EventSubscriptionStatus> eventSubscriptionStatus = eventSubscriptionStatusRepository.findBy(source, component);

        assertThat(eventSubscriptionStatus.isPresent(), is(true));
        assertThat(eventSubscriptionStatus.get().source(), is(source));

        final InOrder inOrder = inOrder(preparedStatement, connection, resultSet);

        inOrder.verify(preparedStatement).setString(1, source);
        inOrder.verify(preparedStatement).setString(2, component);
        inOrder.verify(preparedStatement).executeQuery();

        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldReturnEmptyIfNoEventSubscriptionStatusFoundBySourceAndComponent() throws Exception {

        final String source = "some-source";
        final String component = "some-component";

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SOURCE_AND_COMPONENT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertThat(eventSubscriptionStatusRepository.findBy(source, component).isPresent(), is(false));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setString(1, source);
        inOrder.verify(preparedStatement).setString(2, component);
        inOrder.verify(preparedStatement).executeQuery();

        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventDiscoveryExceptionIfFindingBySourceAndComponentFails() throws Exception {

        final String source = "some-source";
        final String component = "some-component";
        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_BY_SOURCE_AND_COMPONENT_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(sqlException);

        final EventDiscoveryException eventDiscoveryException = assertThrows(
                EventDiscoveryException.class,
                () -> eventSubscriptionStatusRepository.findBy(source, component));

        assertThat(eventDiscoveryException.getCause(), is(sqlException));
        assertThat(eventDiscoveryException.getMessage(), is("Failed to find EventSubscriptionStatus by source: 'some-source, component: 'some-component'"));

        final InOrder inOrder = inOrder(preparedStatement, connection);

        inOrder.verify(preparedStatement).setString(1, source);
        inOrder.verify(preparedStatement).setString(2, component);
        inOrder.verify(preparedStatement).executeQuery();

        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldFindAllEventSubscriptionStatuses() throws Exception {

        final EventSubscriptionStatus eventSubscriptionStatus_1 = new EventSubscriptionStatus(
                "some-source_1",
                "some-component_1",
                of(randomUUID()),
                new UtcClock().now().minusMinutes(2)
        );
        final EventSubscriptionStatus eventSubscriptionStatus_2 = new EventSubscriptionStatus(
                "some-source_2",
                "some-component_2",
                of(randomUUID()),
                new UtcClock().now()
        );

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("source")).thenReturn(eventSubscriptionStatus_1.source(), eventSubscriptionStatus_2.source());
        when(resultSet.getString("component")).thenReturn(eventSubscriptionStatus_1.component(), eventSubscriptionStatus_2.component());
        when(resultSet.getObject("latest_event_id", UUID.class)).thenReturn(eventSubscriptionStatus_1.latestEventId().orElse(null), eventSubscriptionStatus_2.latestEventId().orElse(null));
        when(resultSet.getTimestamp("updated_at")).thenReturn(toSqlTimestamp(eventSubscriptionStatus_1.updatedAt()), toSqlTimestamp(eventSubscriptionStatus_2.updatedAt()));

        final List<EventSubscriptionStatus> eventSubscriptionStatus = eventSubscriptionStatusRepository.findAll();

        assertThat(eventSubscriptionStatus.size(), is(2));

        assertThat(eventSubscriptionStatus.get(0).source(), is(eventSubscriptionStatus_1.source()));
        assertThat(eventSubscriptionStatus.get(0).component(), is(eventSubscriptionStatus_1.component()));
        assertThat(eventSubscriptionStatus.get(0).latestEventId(), is(eventSubscriptionStatus_1.latestEventId()));
        assertThat(eventSubscriptionStatus.get(0).updatedAt(), is(eventSubscriptionStatus_1.updatedAt()));

        assertThat(eventSubscriptionStatus.get(1).source(), is(eventSubscriptionStatus_2.source()));
        assertThat(eventSubscriptionStatus.get(1).component(), is(eventSubscriptionStatus_2.component()));
        assertThat(eventSubscriptionStatus.get(1).latestEventId(), is(eventSubscriptionStatus_2.latestEventId()));
        assertThat(eventSubscriptionStatus.get(1).updatedAt(), is(eventSubscriptionStatus_2.updatedAt()));

        final InOrder inOrder = inOrder(preparedStatement, connection, resultSet);

        inOrder.verify(connection).prepareStatement(FIND_ALL_SQL);
        inOrder.verify(preparedStatement).executeQuery();

        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }

    @Test
    public void shouldThrowEventDiscoveryExceptionUIfFindingAllEventSubscriptionStatusesFails() throws Exception {

        final SQLException sqlException = new SQLException("Ooops");

        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final ResultSet resultSet = mock(ResultSet.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(FIND_ALL_SQL)).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString("source")).thenThrow(sqlException);

        final EventDiscoveryException eventDiscoveryException = assertThrows(EventDiscoveryException.class, () -> eventSubscriptionStatusRepository.findAll());

        assertThat(eventDiscoveryException.getCause(), is(sqlException));
        assertThat(eventDiscoveryException.getMessage(), is("Failed to find all from event-subscription-status table"));

        final InOrder inOrder = inOrder(preparedStatement, connection, resultSet);

        inOrder.verify(connection).prepareStatement(FIND_ALL_SQL);
        inOrder.verify(preparedStatement).executeQuery();

        inOrder.verify(resultSet).close();
        inOrder.verify(preparedStatement).close();
        inOrder.verify(connection).close();
    }
}