package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.util.Collections.unmodifiableList;
import static javax.transaction.Transactional.TxType.MANDATORY;
import static uk.gov.justice.services.common.converter.ZonedDateTimes.fromSqlTimestamp;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.Transactional;

public class CompatibilityModePublishedEventRepository {

    final static String FIND_ALL_SQL = """
                SELECT
                    id,
                    stream_id,
                    position_in_stream,
                    name,
                    payload,
                    metadata,
                    date_created,
                    event_number,
                    previous_event_number
                FROM PUBLISHED_EVENT
                """;

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    @Transactional(MANDATORY)
    public List<LinkedEvent> findAll() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement(FIND_ALL_SQL);
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            final ArrayList<LinkedEvent> events = new ArrayList<>();
            while (resultSet.next()) {

                final UUID eventId = resultSet.getObject("id", UUID.class);
                final UUID streamId = resultSet.getObject("stream_id", UUID.class);
                final long positionInStream = resultSet.getLong("position_in_stream");
                final String name = resultSet.getString("name");
                final String payload = resultSet.getString("payload");
                final String metadata = resultSet.getString("metadata");
                final ZonedDateTime createdAt = fromSqlTimestamp(resultSet.getTimestamp("date_created"));
                final Long eventNumber = resultSet.getLong("event_number");
                final long previousEventNumber = resultSet.getLong("previous_event_number");

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

                events.add(linkedEvent);
            }

            return unmodifiableList(events);

        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to find all events in event_log table", e);
        }
    }
}
