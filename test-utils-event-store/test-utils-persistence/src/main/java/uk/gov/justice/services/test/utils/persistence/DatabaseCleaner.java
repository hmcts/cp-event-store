package uk.gov.justice.services.test.utils.persistence;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import uk.gov.justice.services.jdbc.persistence.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

/**
 * Test utility class for easy cleaning of a context's database. Can clean both buffer tables and
 * the event log table. Plus clean a list of other tables
 * <p>
 * To use:
 *
 * <pre>
 *  {@code
 *     {@literal @}Before
 *     public void cleanTheDatabase() {
 *
 *          databaseCleaner.cleanSubscriptionTable(CONTEXT_NAME);
 *          databaseCleaner.cleanStreamBufferTable(CONTEXT_NAME);
 *          databaseCleaner.cleanEventLogTable(CONTEXT_NAME);
 *          databaseCleaner.cleanViewStoreTables(CONTEXT_NAME, "table_1", "table_2");
 *     }
 *  }
 * </pre>
 */
public class DatabaseCleaner {

    private static final String SQL_PATTERN = "TRUNCATE TABLE %s CASCADE";
    private static final String SET_LATEST_EVENT_ID_ON_EVENT_SUBSCRIPTION_STATUS_TABLE_TO_NULL_SQL = "UPDATE event_subscription_status SET latest_event_id = NULL";

    private final TestJdbcConnectionProvider testJdbcConnectionProvider;

    private static final String VIEW_STORE_DATABASE_NAME = "view-store";
    private static final String EVENT_STORE_DATABASE_NAME = "event-store";
    private static final String SYSTEM_DATABASE_NAME = "system";

    public DatabaseCleaner() {
        this(new TestJdbcConnectionProvider());
    }

    @VisibleForTesting
    DatabaseCleaner(final TestJdbcConnectionProvider testJdbcConnectionProvider) {
        this.testJdbcConnectionProvider = testJdbcConnectionProvider;
    }

    /**
     * Deletes all the data in the 'event_buffer' table
     *
     * @param contextName the name of the context whose tables you are cleaning
     */
    public void cleanStreamBufferTable(final String contextName) {
        cleanViewStoreTables(contextName, "stream_buffer");
    }

    /**
     * Deletes all the data in the 'stream_status' table
     *
     * @param contextName the name of the context whose tables you are cleaning
     */
    public void cleanStreamStatusTable(final String contextName) {
        cleanViewStoreTables(contextName, "stream_status");
    }

    /**
     * Deletes all the data in the 'processed_event' table
     *
     * @param contextName the name of the context whose tables you are cleaning
     */
    public void cleanProcessedEventTable(final String contextName) {
        cleanViewStoreTables(contextName, "processed_event");
    }

    /**
     * Deletes all the data in the 'processed_event' table
     *
     * @param contextName the name of the context whose tables you are cleaning
     */
    public void cleanViewStoreErrorTables(final String contextName) {
        cleanViewStoreTables(contextName, "stream_error_hash", "stream_error");
    }

    /**
     * Deletes all the data in the Event-Store tables
     *
     * @param contextName the name of the context to clean the tables from
     */
    public void cleanEventStoreTables(final String contextName) {
        try (final Connection connection = testJdbcConnectionProvider.getEventStoreConnection(contextName)) {

            truncateTable("event_log", EVENT_STORE_DATABASE_NAME, connection);
            truncateTable("event_stream", EVENT_STORE_DATABASE_NAME, connection);
            truncateTable("publish_queue", EVENT_STORE_DATABASE_NAME, connection);
            truncateTable("published_event", EVENT_STORE_DATABASE_NAME, connection);
        } catch (SQLException e) {
            throw new DataAccessException("Failed to commit or close database connection", e);
        }
    }

    /**
     * Deletes all the data from the specified Event-Store tables
     *
     * @param contextName the name of the context to clean the tables from
     */
    public void cleanEventStoreTables(final String contextName, final String tableName, final String... additionalTableNames) {
        try (final Connection connection = testJdbcConnectionProvider.getEventStoreConnection(contextName)) {

            truncateTable(tableName, EVENT_STORE_DATABASE_NAME, connection);

            for (String additionalTable : additionalTableNames) {
                truncateTable(additionalTable, EVENT_STORE_DATABASE_NAME, connection);
            }

        } catch (SQLException e) {
            throw new DataAccessException("Failed to commit or close database connection", e);
        }
    }

    /**
     * Deprecated from 3.2.0, please use {@link #cleanEventStoreTables(String)} to clean all tables
     * belonging to the event-store.
     * <p>
     * Deletes all the data in the 'event_log' table
     *
     * @param contextName the name of the context who's tables you are cleaning
     */
    @Deprecated
    public void cleanEventLogTable(final String contextName) {
        cleanEventStoreTables(contextName);
    }

    /**
     * Cleans all the tables in the specified list
     *
     * @param contextName          the name of the context who's tables you are cleaning
     * @param tableName            the name of the first table to be cleaned (ensures that there is
     *                             at least one table to be cleaned)
     * @param additionalTableNames the names of any other tables to be cleaned
     */
    public void cleanViewStoreTables(final String contextName, final String tableName, final String... additionalTableNames) {

        final List<String> names = new ArrayList<>();

        names.add(tableName);
        names.addAll(asList(additionalTableNames));

        //noinspection deprecation
        cleanViewStoreTables(contextName, names);
    }

    public void cleanSystemTables(final String contextName) {

        try (final Connection connection = testJdbcConnectionProvider.getSystemConnection(contextName)) {
            truncateTable("stored_command", SYSTEM_DATABASE_NAME, connection);
        } catch (SQLException e) {
            throw new DataAccessException("Failed to commit or close database connection", e);
        }
    }

    /**
     * Resets latest_event_id on event_subscription_status table to null. Table needs source and
     * component pairs in it to make event publishing work (normally run on deployment), but having
     * a rogue latest_event_id from a previous test causes stack traces in logs
     *
     * @param contextName the name of the context whose tables you are cleaning
     */
    public void resetEventSubscriptionStatusTable(final String contextName) {

        try (final Connection connection = testJdbcConnectionProvider.getEventStoreConnection(contextName);
             final PreparedStatement preparedStatement = connection.prepareStatement(SET_LATEST_EVENT_ID_ON_EVENT_SUBSCRIPTION_STATUS_TABLE_TO_NULL_SQL)) {
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new DataAccessException("Failed to set 'event_subscription_status.latest_event_id' to NULL", e);
        }
    }



    /**
     * Cleans all the tables in the specified list
     *
     * @param contextName the name of the context who's tables you are cleaning
     * @param tableNames  a list of names of tables to be cleaned
     * @deprecated use {@link #cleanViewStoreTables(String, String, String...)} instead. It's
     * better.
     */
    @Deprecated
    public void cleanViewStoreTables(final String contextName, final List<String> tableNames) {

        try (final Connection connection = testJdbcConnectionProvider.getViewStoreConnection(contextName)) {
            for (String tableName : tableNames) {
                truncateTable(tableName, VIEW_STORE_DATABASE_NAME, connection);
            }
        } catch (SQLException e) {
            throw new DataAccessException("Failed to commit or close database connection", e);
        }
    }

    private void truncateTable(final String tableName, final String databaseName, final Connection connection) {

        System.out.printf("Truncating table '%s' in %s database\n", tableName, databaseName);
        final String sql = format(SQL_PATTERN, tableName);
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DataAccessException("Failed to delete content from table " + tableName, e);
        }
    }

}
