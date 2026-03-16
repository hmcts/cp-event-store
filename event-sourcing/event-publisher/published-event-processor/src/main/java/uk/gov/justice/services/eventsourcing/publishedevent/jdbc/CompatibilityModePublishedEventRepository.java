package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import static java.lang.String.format;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CompatibilityModePublishedEventRepository {

    final static String INSERT_INTO_PUBLISHED_EVENT_SQL = """
                INSERT INTO published_event (
                    id,
                    stream_id,
                    position_in_stream,
                    name,
                    payload,
                    metadata,
                    date_created,
                    event_number,
                    previous_event_number)
                    (SELECT id,
                        stream_id,
                        position_in_stream,
                        name,
                        payload,
                        jsonb_set(
                                jsonb_set(metadata::jsonb ||  '{"event": {}}'::jsonb, '{event,eventNumber}', to_jsonb(?), true),
                                '{event,previousEventNumber}', to_jsonb(?), true
                            ),
                        date_created,
                        event_number,
                        previous_event_number FROM event_log WHERE id=?)
                """;

    static final String SET_EVENT_NUMBER_SEQUENCE_SQL = """
        SELECT setval('event_sequence_seq', ?);
        """;

    public void insertBatchIntoPublishedEvent(final Connection connection, final List<LinkedEventData> linkDataList) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_INTO_PUBLISHED_EVENT_SQL)) {
            for (final LinkedEventData linkData : linkDataList) {
                preparedStatement.setLong(1, linkData.eventNumber());
                preparedStatement.setLong(2, linkData.previousEventNumber());
                preparedStatement.setObject(3, linkData.eventId());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (final SQLException e) {
            throw new EventPublishingException("Failed to batch insert into published_event table", e);
        }
    }

    public void setEventNumberSequenceTo(final Connection connection, final Long eventNumber) {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(SET_EVENT_NUMBER_SEQUENCE_SQL)) {
            preparedStatement.setLong(1, eventNumber);
            preparedStatement.execute();
        } catch (final SQLException e) {
            throw new EventPublishingException(format("Failed to set event number sequence 'event_sequence_seq' to %d", eventNumber), e);
        }
    }
}