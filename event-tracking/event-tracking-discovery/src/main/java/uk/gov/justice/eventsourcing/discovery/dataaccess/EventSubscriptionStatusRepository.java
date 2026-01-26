package uk.gov.justice.eventsourcing.discovery.dataaccess;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;
import static javax.transaction.Transactional.TxType.REQUIRED;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.eventsourcing.discovery.EventDiscoveryException;
import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class EventSubscriptionStatusRepository {

    static final String UPSERT_EVENT_SUBSCRIPTION_STATUS_SQL = """
                INSERT INTO event_subscription_status (source, component, latest_event_id, latest_known_position, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(source, component)
                DO UPDATE SET
                  latest_event_id = EXCLUDED.latest_event_id,
                  latest_known_position = EXCLUDED.latest_known_position,
                  updated_at = EXCLUDED.updated_at
                """;
    static final String FIND_BY_SOURCE_AND_COMPONENT_SQL = """
                SELECT
                    latest_event_id,
                    latest_known_position,
                    updated_at
                FROM event_subscription_status
                WHERE source = ?
                AND component = ?
                FOR UPDATE SKIP LOCKED
                """;
    static final String FIND_ALL_SQL = """
                SELECT
                    source,
                    component,
                    latest_event_id,
                    latest_known_position,
                    updated_at
                FROM event_subscription_status
                """;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private UtcClock clock;

    @Transactional(REQUIRED)
    public void save(final EventSubscriptionStatus eventSubscriptionStatus) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(UPSERT_EVENT_SUBSCRIPTION_STATUS_SQL)) {

            preparedStatement.setString(1, eventSubscriptionStatus.source());
            preparedStatement.setString(2, eventSubscriptionStatus.component());
            preparedStatement.setObject(3, eventSubscriptionStatus.latestEventId().orElse(null));
            preparedStatement.setLong(4, eventSubscriptionStatus.latestKnownPosition());
            preparedStatement.setTimestamp(5, toSqlTimestamp(eventSubscriptionStatus.updatedAt()));

            preparedStatement.execute();

        } catch (final SQLException e) {
            throw new EventDiscoveryException(format("Failed to upsert %s", eventSubscriptionStatus), e);
        }
    }

    @Transactional(REQUIRED)
    public Optional<EventSubscriptionStatus> findBy(final String source, final String component) {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_SOURCE_AND_COMPONENT_SQL)) {
            preparedStatement.setString(1, source);
            preparedStatement.setString(2, component);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final UUID latestEventId = resultSet.getObject("latest_event_id", UUID.class);
                    final Long latestKnownPosition = resultSet.getLong("latest_known_position");
                    final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));

                    final EventSubscriptionStatus eventSubscriptionStatus = new EventSubscriptionStatus(
                            source,
                            component,
                            ofNullable(latestEventId),
                            latestKnownPosition,
                            updatedAt
                    );

                    return of(eventSubscriptionStatus);
                }
            }

            return empty();

        } catch (final SQLException e) {
            throw new EventDiscoveryException(format("Failed to find EventSubscriptionStatus by source: '%s, component: '%s'", source, component), e);
        }
    }

    @Transactional(REQUIRED)
    public List<EventSubscriptionStatus> findAll() {

        final List<EventSubscriptionStatus> eventSubscriptionStatuses = new ArrayList<>();

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                final String source = resultSet.getString("source");
                final String component = resultSet.getString("component");
                final UUID latestEventId = resultSet.getObject("latest_event_id", UUID.class);
                final Long latestKnownPosition = resultSet.getLong("latest_known_position");
                final ZonedDateTime updatedAt = fromSqlTimestamp(resultSet.getTimestamp("updated_at"));

                final EventSubscriptionStatus eventSubscriptionStatus = new EventSubscriptionStatus(
                        source,
                        component,
                        of(latestEventId),
                        latestKnownPosition,
                        updatedAt
                );

                eventSubscriptionStatuses.add(eventSubscriptionStatus);
            }

            return eventSubscriptionStatuses;

        } catch (final SQLException e) {
            throw new EventDiscoveryException("Failed to find all from event-subscription-status table", e);
        }
    }

    @Transactional(REQUIRED)
    public int insertEmptyRowFor(final String source, final String component) {

        final String sql = """
            INSERT INTO event_subscription_status (source, component, latest_known_position, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source, component) DO NOTHING
            """;
        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setString(1, source);
            preparedStatement.setString(2, component);
            preparedStatement.setLong(3, -1L);
            preparedStatement.setTimestamp(4, toSqlTimestamp(clock.now()));

            return preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventDiscoveryException(format("Failed to insert empty row for source '%s', component '%s'", source, component), e);
        }
    }
}
