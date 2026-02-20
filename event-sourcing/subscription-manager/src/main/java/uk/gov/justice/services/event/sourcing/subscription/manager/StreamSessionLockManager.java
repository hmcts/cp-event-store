package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamSessionLockManager {

    static final String TRY_SESSION_ADVISORY_LOCK_SQL = "SELECT pg_try_advisory_lock(?)";
    static final String SESSION_ADVISORY_UNLOCK_SQL = "SELECT pg_advisory_unlock(?)";

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private Logger logger;

    public Connection openLockConnection() {
        try {
            final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
            connection.setAutoCommit(true);
            return connection;
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to open advisory lock connection", e);
        }
    }

    public boolean tryLockStream(final Connection connection, final UUID streamId) {
        final long lockKey = streamId.getLeastSignificantBits();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(TRY_SESSION_ADVISORY_LOCK_SQL)) {
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

    public void unlockStream(final Connection connection, final UUID streamId) {
        final long lockKey = streamId.getLeastSignificantBits();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(SESSION_ADVISORY_UNLOCK_SQL)) {
            preparedStatement.setLong(1, lockKey);
            preparedStatement.execute();
        } catch (final SQLException e) {
            logger.warn("Failed to release advisory lock for stream '{}': {}", streamId, e.getMessage());
        }
    }

    public void closeQuietly(final Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (final SQLException e) {
                logger.warn("Failed to close advisory lock connection: {}", e.getMessage());
            }
        }
    }
}
