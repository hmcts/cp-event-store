package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.ofNullable;

import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import jakarta.inject.Inject;

@SuppressWarnings("java:S2479")
public class ActiveStreamErrorsRepository {

    private static final String SELECT_ACTIVE_ERRORS_SQL = """
            SELECT
                hash,
                exception_classname,
                cause_classname,
                java_classname,
                java_method,
                java_line_number,
            (
                SELECT COUNT(DISTINCT stream_error.stream_id)
                FROM stream_error
                WHERE stream_error.hash = stream_error_hash.hash
            ) AS affected_stream_count,
            (
                SELECT SUM(stream_status.latest_known_position - stream_status.stream_error_position)
                FROM stream_status, stream_error
                WHERE stream_error.hash = stream_error_hash.hash and stream_error.id = stream_status.stream_error_id
            ) AS affected_event_count
            FROM stream_error_hash;
            """;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    public List<ActiveStreamError> getActiveStreamErrors() {

        try (final Connection connection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_ACTIVE_ERRORS_SQL)) {

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                final List<ActiveStreamError> activeStreamErrors = new ArrayList<>();
                while (resultSet.next()) {
                    final String hash = resultSet.getString("hash");
                    final String exceptionClassname = resultSet.getString("exception_classname");
                    final String causeClassname = resultSet.getString("cause_classname");
                    final String javaClassname = resultSet.getString("java_classname");
                    final String javaMethod = resultSet.getString("java_method");
                    final int javaLineNumber = resultSet.getInt("java_line_number");
                    final int affectedStreamCount = resultSet.getInt("affected_stream_count");
                    final int affectedEventCount = resultSet.getInt("affected_event_count");

                    final ActiveStreamError activeStreamError = new ActiveStreamError(
                            hash,
                            exceptionClassname,
                            ofNullable(causeClassname),
                            javaClassname,
                            javaMethod,
                            javaLineNumber,
                            affectedStreamCount,
                            affectedEventCount
                    );

                    activeStreamErrors.add(activeStreamError);
                }
                
                return activeStreamErrors;
            }
        } catch (final SQLException e) {
            throw new StreamErrorPersistenceException("Failed to get active stream errors from viewstore error tables", e);
        }
    }
}
