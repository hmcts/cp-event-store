package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;

public class EventNumberSequenceDataAccess {

    static final Long ADVISORY_LOCK_KEY = 42L;

    static final String TRANSACTION_LEVEL_ADVISORY_LOCK_SQL = """
            SELECT pg_advisory_xact_lock(?)
            """;

    static final String SELECT_NEXT_EVENT_NUMBER_SQL = """
                    SELECT next_value from next_event_number
                    WHERE id = 'EVENT_NUMBER_SEQUENCE';
                    """;
    static final String UPDATE_NEXT_EVENT_NUMBER_SQL = """
            UPDATE next_event_number SET next_value = ? WHERE id = 'EVENT_NUMBER_SEQUENCE';
            """;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    public Long lockAndGetNextAvailableEventNumber() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement lockPreparedStatement = connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, ADVISORY_LOCK_KEY);
            lockPreparedStatement.execute();
            try (final PreparedStatement queryPreparedStatement = connection.prepareStatement(SELECT_NEXT_EVENT_NUMBER_SQL);
                 final ResultSet resultSet = queryPreparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    return resultSet.getLong("next_value");
                }  else {
                    throw new EventNumberSequenceException("Failed to get next event number. 'next_event_number' table is empty ");
                }
            }
        } catch (final SQLException e) {
            throw new EventNumberSequenceException("Failed to get next event number sequence", e);
        }
    }

    public void updateNextAvailableEventNumberTo(final Long nextEventNumber) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement lockPreparedStatement = connection.prepareStatement(TRANSACTION_LEVEL_ADVISORY_LOCK_SQL)) {
            lockPreparedStatement.setLong(1, ADVISORY_LOCK_KEY);
            lockPreparedStatement.execute();
            try (final PreparedStatement queryPreparedStatement = connection.prepareStatement(UPDATE_NEXT_EVENT_NUMBER_SQL)) {
                queryPreparedStatement.setLong(1, nextEventNumber);
                queryPreparedStatement.execute();
            }
        } catch (final SQLException e) {
            throw new EventNumberSequenceException(format("Failed to update next event number sequence to %d", nextEventNumber), e);
        }
    }
}
