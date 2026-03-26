package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.lang.String.format;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;

public class StreamSessionLockManager {

    static final String TRY_SESSION_ADVISORY_LOCK_SQL = "SELECT pg_try_advisory_lock(?, ?)";
    static final String SESSION_ADVISORY_UNLOCK_SQL = "SELECT pg_advisory_unlock(?, ?)";

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private Logger logger;

    public StreamSessionLock lockStream(final UUID streamId, final String source, final String component) {
        final Connection connection = getAdvisoryConnection();

        StreamSessionLock streamSessionLock = null;
        try {
            final int sourceComponentKey = Objects.hash(source, component);
            final int streamKey = (int) streamId.getLeastSignificantBits();
            streamSessionLock = tryLockStream(connection, sourceComponentKey, streamKey, streamId);
            if (!streamSessionLock.acquired) {
                logger.warn("Advisory lock contention detected for stream '{}', source '{}', component '{}'", streamId, source, component);
            }
            return streamSessionLock;
        } catch (final Exception e) {
            if(streamSessionLock != null) {
                streamSessionLock.close();
            } else {
                closeQuietly(connection);
            }
            throw e;
        }
    }

    private Connection getAdvisoryConnection() {
        try {
            return viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
        } catch (final SQLException e) {
            throw new StreamSessionLockException("Failed to get advisory lock connection", e);
        }
    }

    private StreamSessionLock tryLockStream(final Connection connection, final int sourceComponentKey, final int streamKey, final UUID streamId) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(TRY_SESSION_ADVISORY_LOCK_SQL)) {
            preparedStatement.setInt(1, sourceComponentKey);
            preparedStatement.setInt(2, streamKey);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return new StreamSessionLock(connection, sourceComponentKey, streamKey, streamId, resultSet.getBoolean(1));
                }
                throw new StreamSessionLockException(format("No result returned when acquiring advisory lock for sourceComponentKey %d, streamKey %d", sourceComponentKey, streamKey));
            }
        } catch (final SQLException e) {
            throw new StreamSessionLockException(format("Failed to acquire advisory lock for sourceComponentKey %d, streamKey %d", sourceComponentKey, streamKey), e);
        }
    }

    private void unlockStream(final Connection connection, final int sourceComponentKey, final int streamKey, final UUID streamId) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(SESSION_ADVISORY_UNLOCK_SQL)) {
            preparedStatement.setInt(1, sourceComponentKey);
            preparedStatement.setInt(2, streamKey);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    if (!resultSet.getBoolean(1)) {
                        logger.warn("Advisory unlock returned false for stream '{}' — lock was not held", streamId);
                    }
                } else {
                    logger.warn("No result returned when releasing advisory lock for stream '{}'", streamId);
                }
            }
        } catch (final SQLException e) {
            logger.warn("Failed to release advisory lock for stream '{}': {}", streamId, e.getMessage());
        }
    }

    private void closeQuietly(final Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (final SQLException e) {
                logger.warn("Failed to close advisory lock connection: {}", e.getMessage());
            }
        }
    }

    public class StreamSessionLock implements AutoCloseable {

        private final Connection connection;
        private final int sourceComponentKey;
        private final int streamKey;
        private final UUID streamId;
        private final boolean acquired;

        StreamSessionLock(final Connection connection, final int sourceComponentKey, final int streamKey, final UUID streamId, final boolean acquired) {
            this.connection = connection;
            this.sourceComponentKey = sourceComponentKey;
            this.streamKey = streamKey;
            this.streamId = streamId;
            this.acquired = acquired;
        }

        public boolean isAcquired() {
            return acquired;
        }

        @Override
        public void close() {
            try {
                if (acquired) {
                    unlockStream(connection, sourceComponentKey, streamKey, streamId);
                }
            } finally {
                closeQuietly(connection);
            }
        }
    }
}