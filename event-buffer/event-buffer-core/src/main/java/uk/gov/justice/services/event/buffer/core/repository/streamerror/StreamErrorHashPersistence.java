package uk.gov.justice.services.event.buffer.core.repository.streamerror;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;

public class StreamErrorHashPersistence {

    private static final String FIND_BY_HASH_SQL = """
            SELECT
                hash,
                exception_classname,
                cause_classname,
                java_classname,
                java_method,
                java_line_number
            FROM stream_error_hash
            WHERE hash = ?
            """;

    private static final String FIND_ALL_SQL = """
            SELECT
                hash,
                exception_classname,
                cause_classname,
                java_classname,
                java_method,
                java_line_number
            FROM stream_error_hash
            """;

    private static final String INSERT_HASH_SQL = """
            INSERT INTO stream_error_hash (
                hash,
                exception_classname,
                cause_classname,
                java_classname,
                java_method,
                java_line_number
            )
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
            """;
    private static final String DELETE_HASH_SQL = """
            DELETE FROM stream_error_hash WHERE hash = ?
            """;

    @Inject
    private StreamErrorHashRowMapper streamErrorHashRowMapper;

    public int upsert(final StreamErrorHash streamErrorHash, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_HASH_SQL)) {
            preparedStatement.setString(1, streamErrorHash.hash());
            preparedStatement.setString(2, streamErrorHash.exceptionClassName());
            preparedStatement.setString(3, streamErrorHash.causeClassName().orElse(null));
            preparedStatement.setString(4, streamErrorHash.javaClassName());
            preparedStatement.setString(5, streamErrorHash.javaMethod());
            preparedStatement.setInt(6, streamErrorHash.javaLineNumber());

            return preparedStatement.executeUpdate();
        }
    }

    public Optional<StreamErrorHash> findByHash(final String hash, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_BY_HASH_SQL)) {
            preparedStatement.setString(1, hash);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final StreamErrorHash streamErrorHash = streamErrorHashRowMapper.mapRow(resultSet);
                    return of(streamErrorHash);
                }
            }

            return empty();
        }
    }

    public List<StreamErrorHash> findAll(final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL)) {

            final List<StreamErrorHash> streamErrorHashes = new ArrayList<>();
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final StreamErrorHash streamErrorHash = streamErrorHashRowMapper.mapRow(resultSet);
                    streamErrorHashes.add(streamErrorHash);
                }
            }

            return streamErrorHashes;
        }
    }
    
    public void deleteHash(final String hash, final Connection connection) throws SQLException {

        try (final PreparedStatement preparedStatement = connection.prepareStatement(DELETE_HASH_SQL)) {
            preparedStatement.setString(1, hash);
            preparedStatement.executeUpdate();
        }
    }
}
