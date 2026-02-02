package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
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

    static final String UPDATE_IS_PUBLISHED_FLAG_SQL = """
            UPDATE event_log
            SET is_published = ?
            WHERE id = ?
            """;
    static final String FIND_EVENT_FROM_EVENT_LOG_SQL = """
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
    static final String REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL = """
                DELETE FROM publish_queue
                WHERE event_log_id = (
                    SELECT event_log_id
                    FROM publish_queue
                    ORDER BY date_queued
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING event_log_id
                """;
    static final String DELETE_FROM_PUBLISH_QUEUE_SQL = """
                DELETE FROM publish_queue where event_log_id = ?
                """;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    public Optional<LinkedEvent> findEventFromEventLog(final UUID eventId) {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_EVENT_FROM_EVENT_LOG_SQL)) {

            preparedStatement.setObject(1, eventId);

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {

                if (resultSet.next()) {
                    final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                    final Long positionInStream = resultSet.getObject("position_in_stream", Long.class);
                    final String name = resultSet.getString("name");
                    final String metadata = resultSet.getString("metadata");
                    final String payload = resultSet.getString("payload");
                    final ZonedDateTime createdAt = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                    final Long eventNumber = resultSet.getObject("event_number", Long.class);
                    final Long previousEventNumber = resultSet.getObject("previous_event_number", Long.class);

                    final LinkedEvent linkedEvent = new LinkedEvent(
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

                    return of(linkedEvent);
                }
            }

        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to find event in event_log with id '%s'", eventId), e);
        }

        return empty();
    }

    public Optional<UUID> popNextEventIdFromPublishQueue() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(REMOVE_NEXT_EVENT_ID_FROM_PUBLISH_QUEUE_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            if (resultSet.next()) {
                final UUID eventId = resultSet.getObject(1, UUID.class);
                return of(eventId);
            }

        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find next event id from publish_queue table", e);
        }

        return empty();
    }

    public void setIsPublishedFlag(final UUID eventId, final boolean isPublished) {

        try(final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
            final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_IS_PUBLISHED_FLAG_SQL)) {
            preparedStatement.setBoolean(1, isPublished);
            preparedStatement.setObject(2, eventId);
            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to update 'is_published' on event_log for event id '%s'", eventId), e);
        }
    }
}
