package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AdvisoryLockDataAccess {

    static final String NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_try_advisory_xact_lock(?);
            """;

    /**
     * Attempts to create a transaction level advisory lock using a caller-provided connection.
     *
     * @return true if lock granted, false if not
     */
    public boolean tryNonBlockingTransactionLevelAdvisoryLock(final Connection connection, final Long advisoryLockId) {

        try (final PreparedStatement lockPreparedStatement = connection.prepareStatement(NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, advisoryLockId);

            try (final ResultSet resultSet = lockPreparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    return resultSet.getBoolean(1);
                }

                throw new AdvisoryLockException(format("Unable to obtain non-blocking advisory lock for id '%d'. No result returned when obtaining lock", advisoryLockId));
            }
        } catch (final SQLException e) {
            throw new AdvisoryLockException(format("Failed to obtain non-blocking advisory lock on database with lock key '%d'", advisoryLockId), e);
        }
    }
}