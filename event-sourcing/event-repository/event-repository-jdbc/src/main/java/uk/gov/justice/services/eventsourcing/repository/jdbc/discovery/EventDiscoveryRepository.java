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
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class EventDiscoveryRepository {

    static final String GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_BETWEEN_EVENT_NUMBERS_SQL = """
                    SELECT
                        stream_id,
                        MAX(position_in_stream) AS max_position_in_stream
                    FROM event_log
                    WHERE event_number > ? AND event_number <= ?
                    GROUP BY stream_id
                    ORDER BY MAX(event_number) ASC
            """;
    public static final String SELECT_LE_EVENT_BY_BATCH_SIZE = """
                    SELECT id, event_number
                    FROM event_log
                    WHERE event_number <= ?
                    ORDER BY event_number DESC
                    LIMIT 1
            """;
    public static final String SELECT_EVENT_NUMBER = "SELECT event_number FROM event_log WHERE id = ?";

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Transactional(REQUIRED)
    public List<StreamPosition> getLatestStreamPositionsBetween(final long firstEventNumber, final long lastEventNumber) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(GET_HIGHEST_POSITION_IN_STREAM_FOR_EACH_STREAM_BETWEEN_EVENT_NUMBERS_SQL)) {
            preparedStatement.setLong(1, firstEventNumber);
            preparedStatement.setLong(2, lastEventNumber);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                final List<StreamPosition> streamPositions = new ArrayList<>();
                while (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final Long positionInStream = resultSet.getObject("max_position_in_stream", Long.class);

                    streamPositions.add(new StreamPosition(streamId, positionInStream));
                }

                return streamPositions;
            }
        } catch (final SQLException e) {
            throw new EventStoreEventDiscoveryException(format("Failed to get latest stream positions between eventNumbers '%d' and '%d'", firstEventNumber, lastEventNumber), e);
        }
    }

    @Transactional(REQUIRED)
    public Long getEventNumberFor(final UUID eventId) {
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_EVENT_NUMBER)) {
            preparedStatement.setObject(1, eventId);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong("event_number");
                }
                throw new EventStoreEventDiscoveryException(format("Event with id '%s' not found", eventId));
            }
        } catch (final SQLException e) {
            throw new EventStoreEventDiscoveryException(format("Failed to get event number for eventId '%s'", eventId), e);
        }
    }

    @Transactional(REQUIRED)
    public Optional<EventIdNumber> getLatestEventIdAndNumberAtOffset(final long lastEventNumber, final int batchSize) {
        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_LE_EVENT_BY_BATCH_SIZE)) {
            preparedStatement.setLong(1, lastEventNumber + batchSize);
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return Optional.of(new EventIdNumber(
                            resultSet.getObject("id", UUID.class),
                            resultSet.getLong("event_number")));
                }
                return Optional.empty();
            }
        } catch (final SQLException e) {
            throw new EventStoreEventDiscoveryException(format("Failed to get latest event id and number at offset for lastEventNumber '%d', batchSize '%d'", lastEventNumber, batchSize), e);
        }
    }
}
