package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static jakarta.transaction.Transactional.TxType.MANDATORY;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

public class AdvisoryLockDataAccess {

    static final String BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_advisory_xact_lock(?)
            """;
    static final String NON_BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_try_advisory_xact_lock(?);
            """;

    public void obtainBlockingTransactionLevelAdvisoryLock(final Connection connection, final Long advisoryLockId) {

        try (final PreparedStatement lockPreparedStatement = connection.prepareStatement(BLOCKING_TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, advisoryLockId);
            lockPreparedStatement.execute();
        } catch (final SQLException e) {
            throw new AdvisoryLockException(format("Failed to obtain blocking advisory lock on postgres with lock key '%d'", advisoryLockId), e);
        }
    }

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