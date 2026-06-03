package uk.gov.justice.services.eventstore.management.verification.process;

import uk.gov.justice.services.eventsourcing.source.core.EventStoreDataSourceProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import jakarta.inject.Inject;

public class ActiveStreamIdProvider {

    @Inject
    private EventStoreDataSourceProvider eventStoreDataSourceProvider;

    public Set<UUID> getAllActiveStreamIds() {

        try (final Connection connection = eventStoreDataSourceProvider.getDefaultDataSource().getConnection();
             final PreparedStatement preparedStatement = connection.prepareStatement("SELECT stream_id from event_stream where active = 'true'");
             final ResultSet resultSet = preparedStatement.executeQuery()) {

            final Set<UUID> activeStreamIds = new HashSet<>();

            while (resultSet.next()) {
                final UUID streamId = (UUID) resultSet.getObject("stream_id");

                activeStreamIds.add(streamId);
            }

            return activeStreamIds;

        } catch (final SQLException e) {
            throw new CatchupVerificationException("Failed to get the list of active stream ids from stream_status", e);
        }
    }
}
