package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import javax.inject.Inject;

import org.postgresql.PGConnection;
import org.slf4j.Logger;

public class StreamSessionLockManager {

    static final String TRY_SESSION_ADVISORY_LOCK_SQL = "SELECT pg_try_advisory_lock(?)";
    static final String SESSION_ADVISORY_UNLOCK_SQL = "SELECT pg_advisory_unlock(?)";

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private Logger logger;

    public Connection getJtaConnection() {
        try {
            final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
            logger.trace("CONN-TRACE [getJtaConnection] acquired handle={}, PID={}", System.identityHashCode(connection), getBackendPid(connection));
            return connection;
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to get JTA connection for advisory lock", e);
        }
    }

    private int getBackendPid(final Connection connection) {
        try (final PreparedStatement ps = connection.prepareStatement("SELECT pg_backend_pid()");
             final ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : -1;
        } catch (final SQLException e) {
            return -1;
        }
    }

    /**
     * Unwraps the physical PostgreSQL connection from the IronJacamar WrappedConnection.
     * The returned Connection is the raw PgConnection (which implements both Connection
     * and PGConnection), bypassing IronJacamar's JTA restrictions for advisory lock operations.
     */
    public Connection unwrapPhysicalConnection(final Connection wrappedConnection) {
        try {
            return (Connection) wrappedConnection.unwrap(PGConnection.class);
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to unwrap physical connection from JTA connection", e);
        }
    }

    public boolean tryLockStream(final Connection physicalConnection, final UUID streamId) {
        final long lockKey = toLockKey(streamId);

        try (final PreparedStatement preparedStatement = physicalConnection.prepareStatement(TRY_SESSION_ADVISORY_LOCK_SQL)) {
            preparedStatement.setLong(1, lockKey);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getBoolean(1);
                }
                throw new StreamSessionLockException(format("No result returned when acquiring advisory lock for stream '%s'", streamId));
            }
        } catch (final SQLException e) {
            throw new StreamSessionLockException(format("Failed to acquire advisory lock for stream '%s'", streamId), e);
        }
    }

    public void unlockStream(final Connection physicalConnection, final UUID streamId) {
        final long lockKey = toLockKey(streamId);

        try (final PreparedStatement preparedStatement = physicalConnection.prepareStatement(SESSION_ADVISORY_UNLOCK_SQL)) {
            preparedStatement.setLong(1, lockKey);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next() && !resultSet.getBoolean(1)) {
                    logger.warn("Advisory lock for stream '{}' was not held when unlock was attempted", streamId);
                }
            }
        } catch (final SQLException e) {
            logger.warn("Failed to release advisory lock for stream '{}': {}", streamId, e.getMessage());
        }
    }

    private long toLockKey(final UUID streamId) {
        return streamId.getLeastSignificantBits();
    }

    public void beginDirectTransaction(final Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to begin direct transaction on physical connection", e);
        }
    }

    public void commitDirectTransaction(final Connection connection) {
        try {
            connection.commit();
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to commit direct transaction on physical connection", e);
        }
    }

    public void rollbackDirectTransaction(final Connection connection) {
        try {
            connection.rollback();
        } catch (final SQLException e) {
            logger.warn("Failed to rollback direct transaction: {}", e.getMessage());
        }
    }

    public void restoreAutoCommit(final Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (final SQLException e) {
            logger.warn("Failed to restore autoCommit on connection: {}", e.getMessage());
        }
    }

    public void closeHandle(final Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (final SQLException e) {
                logger.warn("Failed to close connection handle: {}", e.getMessage());
            }
        }
    }
}
