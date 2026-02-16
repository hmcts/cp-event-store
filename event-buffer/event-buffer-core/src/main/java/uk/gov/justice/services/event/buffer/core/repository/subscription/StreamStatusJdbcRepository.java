package uk.gov.justice.services.event.buffer.core.repository.subscription;

import static java.lang.String.format;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.toSqlTimestamp;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.jdbc.persistence.JdbcRepositoryException;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapper;
import uk.gov.justice.services.jdbc.persistence.PreparedStatementWrapperFactory;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;

@ApplicationScoped
public class StreamStatusJdbcRepository {

    /**
     * Column Names
     */
    private static final String PRIMARY_KEY_ID = "stream_id";
    private static final String LATEST_POSITION_COLUMN = "position";
    private static final String SOURCE = "source";
    private static final String COMPONENT = "component";

    /**
     * Statements
     */
    private static final String SELECT_BY_STREAM_ID_AND_SOURCE_SQL = "SELECT stream_id, position, source, component FROM stream_status WHERE stream_id=? AND component=? AND source in (?,'unknown') FOR UPDATE";
    private static final String INSERT_SQL = "INSERT INTO stream_status (position, stream_id, source, component, discovered_at) VALUES (?, ?, ?, ?, ?)";
    private static final String INSERT_ON_CONFLICT_DO_NOTHING_SQL = INSERT_SQL + " ON CONFLICT DO NOTHING";
    private static final String UPDATE_SQL = "UPDATE stream_status SET position=?,source=?,component=? WHERE stream_id=? and component=? and source in (?,'unknown')";
    private static final String UPDATE_UNKNOWN_SOURCE_SQL = "UPDATE stream_status SET source=?, component=? WHERE stream_id=? and source = 'unknown'";

    @Inject
    private PreparedStatementWrapperFactory preparedStatementWrapperFactory;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private UtcClock clock;

    public StreamStatusJdbcRepository() {
    }

    public StreamStatusJdbcRepository(
            final ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider,
            final PreparedStatementWrapperFactory preparedStatementWrapperFactory,
            final UtcClock clock) {
        this.viewStoreJdbcDataSourceProvider = viewStoreJdbcDataSourceProvider;
        this.preparedStatementWrapperFactory = preparedStatementWrapperFactory;
        this.clock = clock;
    }


    /**
     * Insert the given Subscription into the subscription table.
     *
     * @param subscription the status of the stream to insert
     */
    public void insert(final Subscription subscription) {
        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(viewStoreDataSource, INSERT_SQL)) {
            ps.setLong(1, subscription.getPosition());
            ps.setObject(2, subscription.getStreamId());
            ps.setString(3, subscription.getSource());
            ps.setString(4, subscription.getComponent());
            ps.setTimestamp(5, toSqlTimestamp(clock.now()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while storing status of the stream: %s", subscription), e);
        }
    }


    /**
     * Tries to insert if database is PostgresSQL and version&gt;=9.5. Uses PostgreSQl-specific sql
     * clause. Does not fail if status for the given stream already exists
     *
     * @param subscription the status of the stream to insert
     */
    public void insertOrDoNothing(final Subscription subscription) {

        final DataSource viewStoreDataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        try (final PreparedStatementWrapper preparedStatement = preparedStatementWrapperFactory.preparedStatementWrapperOf(viewStoreDataSource, INSERT_ON_CONFLICT_DO_NOTHING_SQL)) {
            preparedStatement.setLong(1, subscription.getPosition());
            preparedStatement.setObject(2, subscription.getStreamId());
            preparedStatement.setString(3, subscription.getSource());
            preparedStatement.setString(4, subscription.getComponent());
            preparedStatement.setTimestamp(5, toSqlTimestamp(clock.now()));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while storing status of the stream in PostgreSQL: %s", subscription), e);
        }

    }

    /**
     * Insert the given Subscription into the stream status table.
     *
     * @param subscription the event to insert
     */
    public void update(final Subscription subscription) {

        final DataSource dataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, UPDATE_SQL)) {
            ps.setLong(1, subscription.getPosition());
            ps.setString(2, subscription.getSource());
            ps.setObject(3, subscription.getComponent());
            ps.setObject(4, subscription.getStreamId());
            ps.setObject(5, subscription.getComponent());
            ps.setString(6, subscription.getSource());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while updating status of the stream: %s", subscription), e);
        }
    }

    /**
     * Returns a Stream of {@link Subscription} for the given stream streamId.
     *
     * @param streamId  streamId of the stream.
     * @param component
     * @return a {@link Subscription}.
     */
    public Optional<Subscription> findByStreamIdAndSource(final UUID streamId, final String source, final String component) {

        final DataSource dataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, SELECT_BY_STREAM_ID_AND_SOURCE_SQL)) {
            ps.setObject(1, streamId);
            ps.setObject(2, component);
            ps.setObject(3, source);
            return subscriptionFrom(ps);

        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while looking up status of the stream: %s", streamId), e);
        }
    }

    private Optional<Subscription> subscriptionFrom(final PreparedStatementWrapper ps) throws SQLException {
        final ResultSet resultSet = ps.executeQuery();
        return resultSet.next()
                ? Optional.of(entityFrom(resultSet))
                : Optional.empty();

    }

    protected Subscription entityFrom(final ResultSet resultSet) throws SQLException {
        return new Subscription(
                resultSet.getObject(PRIMARY_KEY_ID, UUID.class),
                resultSet.getObject(LATEST_POSITION_COLUMN, Long.class),
                resultSet.getString(SOURCE),
                resultSet.getString(COMPONENT));
    }

    public void updateSource(final UUID streamId, final String source, final String component) {
        final DataSource dataSource = viewStoreJdbcDataSourceProvider.getDataSource();
        try (final PreparedStatementWrapper ps = preparedStatementWrapperFactory.preparedStatementWrapperOf(dataSource, UPDATE_UNKNOWN_SOURCE_SQL)) {
            ps.setString(1, source);
            ps.setObject(2, component);
            ps.setObject(3, streamId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcRepositoryException(format("Exception while updating unknown source of the stream: %s", streamId), e);
        }
    }
}
