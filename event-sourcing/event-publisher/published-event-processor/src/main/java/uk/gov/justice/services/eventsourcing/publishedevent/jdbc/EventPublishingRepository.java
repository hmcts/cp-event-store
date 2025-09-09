package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;

public class EventPublishingRepository {

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    public Optional<PublishedEvent> findEventFromEventLog(final UUID eventId) {

        final String sql = """
                SELECT
                    stream_id,
                    position_in_stream,
                    name,
                    payload,
                    metadata,
                    date_created,
                    event_number,
                    previous_event_number
                FROM event_log
                WHERE id = ?
                """;

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            preparedStatement.setObject(1, eventId);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final long positionInStream = resultSet.getLong("position_in_stream");
                    final String name = resultSet.getString("name");
                    final String metadata = resultSet.getString("metadata");
                    final String payload = resultSet.getString("payload");
                    final ZonedDateTime createdAt = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final long eventNumber = resultSet.getLong("event_number");
                    final long previousEventNumber = resultSet.getLong("previous_event_number");

                    final PublishedEvent publishedEvent = new PublishedEvent(
                            eventId,
                            streamId,
                            positionInStream,
                            name,
                            metadata,
                            payload,
                            createdAt,
                            eventNumber,
                            previousEventNumber
                    );

                    return of(publishedEvent);
                }
            }

        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to find event in event_log with id '%s'", eventId), e);
        }

        return empty();
    }

    public Optional<UUID> getNextEventIdFromPublishQueue() {

        final String sql = """
                SELECT event_log_id
                FROM publish_queue
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """;

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                final UUID eventId = resultSet.getObject(1, UUID.class);
                return of(eventId);
            }

        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find event id in publish_queue table", e);
        }

        return empty();
    }

    public void removeFromPublishQueue(final UUID eventId) {

        final String sql = """
                DELETE FROM publish_queue where event_log_id = ?
                """;

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setObject(1, eventId);
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to delete from publish_queue table. eventId: '%s'", eventId), e);
        }
    }
}
