package uk.gov.justice.services.eventsourcing.repository.jdbc.discovery;

import static java.lang.String.format;
import static javax.transaction.Transactional.TxType.REQUIRED;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class EventDiscoveryRepository {

    static final String GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_SQL = """
            SELECT
                stream_id,
                MAX(position_in_stream) AS max_position_in_stream
            FROM event_log
            WHERE event_number >
                (SELECT event_number FROM event_log WHERE id = ?)
            GROUP BY stream_id
            LIMIT ?;
            """;
    static final String GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_FROM_EVENT_NUMBER_SQL = """
            SELECT
                stream_id,
                MAX(position_in_stream) AS max_position_in_stream
            FROM event_log
            WHERE event_number > ?
            GROUP BY stream_id
            LIMIT ?;
            """;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Transactional(REQUIRED)
    public List<StreamPosition> getLatestStreamPositions(final Long eventNumber, final int batchSize) {

        try(final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_FROM_EVENT_NUMBER_SQL)) {
            preparedStatement.setLong(1, eventNumber);
            preparedStatement.setInt(2, batchSize);

            try(final ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<StreamPosition> streamPositions = new ArrayList<>();
                while (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final Long positionInStream = resultSet.getObject("max_position_in_stream", Long.class);

                    streamPositions.add(new StreamPosition(streamId, positionInStream));
                }

                return streamPositions;
            }
        } catch (final SQLException e) {
            throw new EventStoreEventDiscoveryException(format("Failed to get latest stream positions for eventNumber '%d', batchSize '%d'", eventNumber, batchSize), e);
        }
    }

    @Transactional(REQUIRED)
    public List<StreamPosition> getLatestStreamPositions(final UUID eventId, final int batchSize) {

        try(final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_SQL)) {
            preparedStatement.setObject(1, eventId);
            preparedStatement.setInt(2, batchSize);

            try(final ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<StreamPosition> streamPositions = new ArrayList<>();
                while (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final Long positionInStream = resultSet.getObject("max_position_in_stream", Long.class);

                    streamPositions.add(new StreamPosition(streamId, positionInStream));
                }

                return streamPositions;
            }
        } catch (final SQLException e) {
            throw new EventStoreEventDiscoveryException(format("Failed to get latest stream positions for eventId '%s', batchSize '%d'", eventId, batchSize), e);
        }
    }
}
